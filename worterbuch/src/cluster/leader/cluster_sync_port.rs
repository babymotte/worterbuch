/*
 *  Helper functions for cluster sync port of leader mode
 *
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use crate::{
    Config,
    cluster::{
        ClusterStateChangeReceiver,
        leader::virtual_proxy_server::{VirtualProxyServer, VirtualServerMessage},
        protocol::{
            ClusterStateChange, Handshake, LeaderMessage, LeaderWelcome, ProxyId, ProxyMessage,
            StateSync,
        },
    },
    server::common::{
        CloneableWbApi,
        protocol::{self, LazyBroadcaster},
    },
    worterbuch_version,
};
use hashbrown::HashMap;
use miette::{Context, IntoDiagnostic, Result, bail, ensure};
use serde_json::json;
use std::{
    io::{self},
    net::SocketAddr,
    ops::ControlFlow,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    spawn,
    sync::{mpsc, oneshot},
};
use tosub::Subsystem;
use totils::while_select;
use tracing::{debug, error, info, instrument, trace, warn};
use worterbuch_common::{
    WorterbuchVersion,
    protocol::v1::{Err, ErrorCode, ServerMessage},
    socket::{TcpSocketConfig, create_tcp_server_socket},
    write_line_and_flush,
};

/// The currently active connection of each proxy instance, identified by the proxy's ID
#[derive(Debug, Clone, Default)]
struct ProxySessions(Arc<Mutex<HashMap<ProxyId, (SocketAddr, Subsystem)>>>);

impl ProxySessions {
    fn update(
        &self,
        proxy_id: ProxyId,
        proxy: (SocketAddr, Subsystem),
    ) -> Option<(SocketAddr, Subsystem)> {
        trace!(enter = "update");
        let previous = self
            .0
            .lock()
            .expect("mutex is poisoned")
            .insert(proxy_id, proxy);
        trace!(exit = "update");
        previous
    }

    fn release_own(&self, proxy_id: ProxyId, proxy_addr: SocketAddr) {
        trace!(enter = "release_own");
        let mut sessions = self.0.lock().expect("mutex is poisoned");
        // the session may already have been taken over by a newer connection of the same proxy
        if sessions
            .get(&proxy_id)
            .is_some_and(|(session_addr, _)| *session_addr == proxy_addr)
        {
            debug!("Releasing currently registered proxy session {}", proxy_id);
            sessions.remove(&proxy_id);
        } else {
            debug!(
                "Currently registered proxy session is not the one that just got closed, cannot release."
            );
        }
        trace!(exit = "release_own");
    }
}

pub async fn run_cluster_sync_port(
    subsys: Subsystem,
    config: Config,
    wb: CloneableWbApi,
    on_follower_connected: mpsc::Sender<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    on_follower_disconnected: mpsc::Sender<SocketAddr>,
    port: u16,
) -> Result<()> {
    let ip = config
        .tcp_endpoint
        .clone()
        .expect("no tcp bind address configured")
        .bind_addr;

    info!("Starting cluster sync endpoint at {}:{} …", ip, port);

    let socket_config = TcpSocketConfig::from(&config);
    let std_socket =
        create_tcp_server_socket(ip, port, socket_config).wrap_err("failed to create sync port")?;
    let socket = TcpListener::from_std(std_socket)
        .into_diagnostic()
        .wrap_err("failed to convert std socket to tokio socket")?;

    let proxy_sessions = ProxySessions::default();

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        client = socket.accept() => accecpt_client(client, &subsys, &config, &wb, on_follower_connected.clone(), on_follower_disconnected.clone(), &proxy_sessions),
    }

    drop(socket);

    info!("Cluster sync port closed.");

    Ok(())
}

#[instrument(skip_all)]
fn accecpt_client(
    client: io::Result<(TcpStream, SocketAddr)>,
    subsys: &Subsystem,
    config: &Config,
    wb: &CloneableWbApi,
    on_follower_connected: mpsc::Sender<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    on_follower_disconnected: mpsc::Sender<SocketAddr>,
    proxy_sessions: &ProxySessions,
) -> ControlFlow<()> {
    trace!(enter = "accecpt_client");
    match client {
        Ok(client) => {
            // TODO reject connections from clients that are not cluster peers or have proper proxy authentication
            let name = format!("follower-proxy/{}", client.1);
            serve(
                subsys,
                client,
                on_follower_connected,
                on_follower_disconnected,
                config.clone(),
                wb.named(name),
                proxy_sessions.clone(),
            );
            trace!(exit = "accecpt_client");
            return ControlFlow::Continue(());
        }
        Err(e) => {
            error!("Error accepting follower connections: {e}");
            return ControlFlow::Break(());
        }
    };
}

#[instrument(skip_all, fields(client = %client.1))]
fn serve(
    subsys: &Subsystem,
    client: (TcpStream, SocketAddr),
    on_follower_connected: mpsc::Sender<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    on_follower_disconnected: mpsc::Sender<SocketAddr>,
    config: Config,
    wb: CloneableWbApi,
    proxy_sessions: ProxySessions,
) {
    info!("Follower {} connected.", client.1);

    subsys.spawn(client.1.to_string(), async move |s| {
        let follower_addr = client.1;
        if let Err(e) = follower_serve_loop(
            &s,
            client.0,
            client.1,
            on_follower_connected,
            config,
            wb,
            proxy_sessions,
        )
        .await
        {
            error!("Error in follower serve loop: {e}");
            eprintln!("{e:?}");
        }

        let _ = on_follower_disconnected
            .try_send(follower_addr)
            .expect("follower disconnected channel is clogged");

        s.request_local_shutdown_because("TCP connection to follower/proxy closed");
    });
}

async fn follower_serve_loop(
    subsys: &Subsystem,
    tcp_stream: TcpStream,
    follower: SocketAddr,
    on_follower_connected: mpsc::Sender<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    config: Config,
    worterbuch: CloneableWbApi,
    proxy_sessions: ProxySessions,
) -> miette::Result<()> {
    let (send_tx, send_rx) = mpsc::channel(config.channel_buffer_size);
    let mut proxy_server = VirtualProxyServer {
        subsys: subsys.clone(),
        clients: HashMap::new(),
        worterbuch: worterbuch.named("server/virtual-proxy"),
        config: config.clone(),
        proxy_address: follower,
        send_tx,
    };
    let mut proxy_id = None;

    let res = follower_session(
        subsys,
        tcp_stream,
        follower,
        on_follower_connected,
        config,
        &mut proxy_server,
        send_rx,
        &proxy_sessions,
        &mut proxy_id,
    )
    .await;

    // no matter how the session ended, all of its clients must be disconnected before this task ends,
    // a new connection of the same proxy waits for this task to end before registering its clients
    proxy_server.disconnect_all().await;

    if let Some(proxy_id) = proxy_id {
        proxy_sessions.release_own(proxy_id, follower);
    }

    res
}

#[allow(clippy::too_many_arguments)]
async fn follower_session(
    subsys: &Subsystem,
    tcp_stream: TcpStream,
    follower: SocketAddr,
    on_follower_connected: mpsc::Sender<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    config: Config,
    proxy_server: &mut VirtualProxyServer,
    mut send_rx: mpsc::Receiver<VirtualServerMessage>,
    proxy_sessions: &ProxySessions,
    proxy_id: &mut Option<ProxyId>,
) -> miette::Result<()> {
    let (socket_rx, mut socket_tx) = tcp_stream.into_split();
    let mut proxy_messages = BufReader::new(socket_rx).lines();

    send_welcome(subsys, &mut socket_tx, follower, &config).await?;
    let handshake = receive_handshake(&mut proxy_messages, follower).await?;

    let is_proxy =
        process_handshake(handshake, &config, proxy_server, proxy_sessions, proxy_id).await?;

    let (sync_tx, sync_rx) = oneshot::channel();
    on_follower_connected
        .send((sync_tx, follower, is_proxy))
        .await
        .into_diagnostic()
        .wrap_err("failed to forward follower connected event")?;

    let (state, mut commands) = sync_rx.await.into_diagnostic()?;

    send_initial_state(subsys, &mut socket_tx, follower, &config, state).await?;

    while_select! {
        _ = proxy_server.subsys.shutdown_requested() => break,
        recv = commands.recv() => forward_change_to_follower(&proxy_server.subsys, recv, &mut socket_tx, &config, follower).await.wrap_err("error forwarding change message to follower/proxy")?,
        recv = proxy_messages.next_line() => proxy_server.process_proxy_message(recv, follower).await.wrap_err("error processing proxy message")?,
        recv = send_rx.recv() => forward_response_to_proxy(&proxy_server.subsys, recv, &mut socket_tx, &config, follower).await.wrap_err("error forwarding response message to proxy")?,
    }

    info!("TCP connection to follower/proxy {} closed.", follower);

    Ok(())
}

async fn send_welcome(
    subsys: &Subsystem,
    tcp_stream: &mut OwnedWriteHalf,
    follower: SocketAddr,
    config: &Config,
) -> miette::Result<()> {
    trace!(enter = "send_welcome");
    let authentication_required = config.auth_token_key.is_some();
    let version = worterbuch_version();

    write_line_and_flush(
        || subsys.shutdown_requested(),
        LeaderMessage::Welcome(LeaderWelcome {
            version,
            authentication_required,
        }),
        tcp_stream,
        config.send_timeout,
    )
    .await
    .into_diagnostic()
    .wrap_err_with(|| {
        format!(
            "could not send current state to follower/proxy {}",
            follower
        )
    })?;
    trace!(exit = "send_welcome");
    Ok(())
}

async fn receive_handshake(
    tcp_stream: &mut Lines<BufReader<OwnedReadHalf>>,
    follower: SocketAddr,
) -> miette::Result<Handshake> {
    trace!(enter = "receive_handshake");
    let handshake = match tcp_stream.next_line().await {
        Ok(Some(line)) => {
            debug!("Received handshake message from follower/proxy: {line}");

            let msg: ProxyMessage = serde_json::from_str(&line)
                .into_diagnostic()
                .wrap_err("could not parse follower/proxy message")?;

            match msg {
                ProxyMessage::Handshake(handshake) => Ok(handshake),
                msg => {
                    trace!(exit = "receive_handshake", error = true);
                    bail!(
                        "expected handshake message from follower/proxy {}, but got:\n{:#?}",
                        follower,
                        msg
                    );
                }
            }
        }
        Ok(None) => {
            trace!(exit = "receive_handshake", error = true);
            bail!(
                "connection to follower/proxy {} closed before handshake",
                follower
            );
        }
        Err(e) => {
            trace!(exit = "receive_handshake", error = true);
            bail!(
                "error receiving handshake message from follower/proxy {}: {}",
                follower,
                e
            );
        }
    };
    trace!(exit = "receive_handshake");
    handshake
}

async fn process_handshake(
    handshake: Handshake,
    config: &Config,
    proxy_server: &mut VirtualProxyServer,
    proxy_sessions: &ProxySessions,
    proxy_id: &mut Option<ProxyId>,
) -> miette::Result<bool> {
    trace!(enter = "process_handshake");
    check_version(handshake.version())
        .wrap_err("version mismatch between leader and follower/proxy")?;

    authenticate(handshake.auth_token(), config)
        .wrap_err("could not authenticate follower/proxy")?;

    if let Handshake::Proxy(proxy_handshake) = handshake {
        let id = proxy_handshake.proxy_id;
        *proxy_id = Some(id);
        take_over_proxy_session(proxy_sessions, id, proxy_server).await;

        let client_txs = proxy_server
            .register_clients(&proxy_handshake.connected_clients)
            .await?;
        let updated_locks = proxy_server.restore_locks(proxy_handshake.locks).await?;

        for (client_id, keys) in &updated_locks.lost {
            let Some(tx) = client_txs.get(client_id) else {
                error!(
                    "No client message broadcaster for client {client_id} found, cannot notify about lost locks."
                );
                continue;
            };
            for (tid, _) in keys {
                let msg = ServerMessage::Err(Err {
                    transaction_id: *tid,
                    error_code: ErrorCode::LockLost,
                    metadata: json!("lock lost").to_string(),
                });
                let _ = tx.lazy_send(msg).await;
            }
        }

        for (client_id, pending_locks) in updated_locks.pending {
            let Some(client) = client_txs.get(&client_id) else {
                error!(
                    "No client message broadcaster for client {client_id} found, cannot notify about lost locks."
                );
                continue;
            };
            for (transaction_id, _, acquired_rx, lost_rx) in pending_locks {
                spawn(protocol::forward_lock_acquired(
                    client.to_owned(),
                    transaction_id,
                    acquired_rx,
                    lost_rx,
                ));
            }
        }

        trace!(exit = "process_handshake");
        Ok(true)
    } else {
        trace!(exit = "process_handshake");
        Ok(false)
    }
}

/// Registers this connection as the active session of the proxy. If the proxy still has a previous connection
/// (e.g. one that broke without the leader noticing yet), that connection is closed and all of its clients are
/// disconnected before this returns, so that the clients of the new connection can be registered without
/// colliding with their stale registrations.
async fn take_over_proxy_session(
    proxy_sessions: &ProxySessions,
    proxy_id: ProxyId,
    proxy_server: &VirtualProxyServer,
) {
    trace!(enter = "take_over_proxy_session");
    let previous = proxy_sessions.update(
        proxy_id,
        (proxy_server.proxy_address, proxy_server.subsys.clone()),
    );

    let Some((previous_addr, previous_subsys)) = previous else {
        return;
    };

    info!(
        "Proxy {proxy_id} re-connected from {} while its previous connection from {previous_addr} is still open, closing previous connection …",
        proxy_server.proxy_address
    );
    previous_subsys.request_local_shutdown_because(format!(
        "proxy {proxy_id} re-connected from {}",
        proxy_server.proxy_address
    ));
    if let Err(e) = previous_subsys.join().await {
        warn!(
            "Previous connection of proxy {proxy_id} from {previous_addr} did not shut down cleanly: {e}"
        );
    } else {
        info!("Previous connection of proxy {proxy_id} from {previous_addr} closed.");
    }
    trace!(exit = "take_over_proxy_session");
}

fn check_version(version: &WorterbuchVersion) -> miette::Result<()> {
    ensure!(
        version == &worterbuch_version(),
        "follower/proxy version mismatch: expected {}, got {}",
        worterbuch_version(),
        version
    );
    Ok(())
}

fn authenticate(auth_token: Option<&str>, config: &Config) -> miette::Result<()> {
    trace!(enter = "authenticate");
    match &config.auth_token_key {
        Some(key) => authenticate_against_key(auth_token, key),
        None => Ok(()),
    }?;
    trace!(exit = "authenticate");
    Ok(())
}

fn authenticate_against_key(_auth_token: Option<&str>, _key: &str) -> miette::Result<()> {
    // TODO
    Ok(())
}

async fn send_initial_state(
    subsys: &Subsystem,
    tcp_stream: &mut OwnedWriteHalf,
    follower: SocketAddr,
    config: &Config,
    state: StateSync,
) -> miette::Result<()> {
    trace!(enter = "send_initial_state");
    write_line_and_flush(
        || subsys.shutdown_requested(),
        LeaderMessage::Init(state),
        tcp_stream,
        config.send_timeout,
    )
    .await
    .into_diagnostic()
    .wrap_err_with(|| {
        format!(
            "could not send current state to follower/proxy {}",
            follower
        )
    })?;
    trace!(exit = "send_initial_state");
    Ok(())
}

async fn forward_change_to_follower(
    subsys: &Subsystem,
    recv: Option<ClusterStateChange>,
    socket_tx: &mut OwnedWriteHalf,
    config: &Config,
    follower: SocketAddr,
) -> miette::Result<ControlFlow<()>> {
    trace!(enter = "forward_change_to_follower");
    match recv {
        Some(change) => {
            write_line_and_flush(
                || subsys.shutdown_requested(),
                LeaderMessage::Mut(change),
                socket_tx,
                config.send_timeout,
            )
            .await
            .wrap_err_with(|| format!("could not write command to follower/proxy {}", follower))?;
        }
        None => {
            trace!(exit = "forward_change_to_follower");
            return Ok(ControlFlow::Break(()));
        }
    }

    trace!(exit = "forward_change_to_follower");
    Ok(ControlFlow::Continue(()))
}

async fn forward_response_to_proxy(
    subsys: &Subsystem,
    recv: Option<VirtualServerMessage>,
    socket_tx: &mut OwnedWriteHalf,
    config: &Config,
    follower: SocketAddr,
) -> miette::Result<ControlFlow<()>> {
    trace!(enter = "forward_response_to_proxy");
    match recv {
        Some(VirtualServerMessage::ServerMessage((client_id, server_message))) => {
            write_line_and_flush(
                || subsys.shutdown_requested(),
                LeaderMessage::ClientResponse(client_id, server_message),
                socket_tx,
                config.send_timeout,
            )
            .await
            .wrap_err_with(|| format!("could not write command to follower/proxy {}", follower))?;
        }
        Some(VirtualServerMessage::Disconnect(client_id)) => {
            write_line_and_flush(
                || subsys.shutdown_requested(),
                LeaderMessage::EjectClient(client_id),
                socket_tx,
                config.send_timeout,
            )
            .await
            .wrap_err_with(|| format!("could not write command to follower/proxy {}", follower))?;
        }
        None => {
            trace!(exit = "forward_response_to_proxy");
            return Ok(ControlFlow::Break(()));
        }
    }

    trace!(exit = "forward_response_to_proxy");
    Ok(ControlFlow::Continue(()))
}
