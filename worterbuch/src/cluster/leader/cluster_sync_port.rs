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
    Config, auth,
    cluster::{
        ClusterStateChangeReceiver,
        leader::virtual_proxy_server::{VirtualProxyServer, VirtualServerMessage},
        protocol::{
            ClusterStateChange, Handshake, LeaderMessage, LeaderWelcome, ProxyId, ProxyMessage,
            StateSync,
        },
    },
    server::common::{CloneableWbApi, protocol},
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
    io::{AsyncBufReadExt, BufReader, BufWriter, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
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

pub type FollowerConnected = (
    oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
    SocketAddr,
    bool,
);
pub type OnFollowerConnected = mpsc::Sender<FollowerConnected>;
pub type OnFollowerDisconnected = mpsc::Sender<SocketAddr>;

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
    on_follower_connected: OnFollowerConnected,
    on_follower_disconnected: OnFollowerDisconnected,
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
    on_follower_connected: OnFollowerConnected,
    on_follower_disconnected: OnFollowerDisconnected,
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
            ControlFlow::Continue(())
        }
        Err(e) => {
            error!("Error accepting follower connections: {e}");
            ControlFlow::Break(())
        }
    }
}

#[instrument(skip_all, fields(client = %client.1))]
fn serve(
    subsys: &Subsystem,
    client: (TcpStream, SocketAddr),
    on_follower_connected: OnFollowerConnected,
    on_follower_disconnected: OnFollowerDisconnected,
    config: Config,
    wb: CloneableWbApi,
    proxy_sessions: ProxySessions,
) {
    info!("Follower {} connected.", client.1);

    subsys.spawn(client.1.to_string(), move |s| async move {
        let client_handler = SyncClientHandler {
            subsys: s,
            follower_addr: client.1,
            on_follower_connected,
            on_follower_disconnected,
            config,
            wb,
            proxy_sessions,
        };
        if let Err(e) = client_handler.run(client.0).await {
            error!("Error in sync client handler: {e}");
        }
    });
}

struct SyncClientHandler {
    subsys: Subsystem,
    follower_addr: SocketAddr,
    on_follower_connected: OnFollowerConnected,
    on_follower_disconnected: OnFollowerDisconnected,
    config: Config,
    wb: CloneableWbApi,
    proxy_sessions: ProxySessions,
}

impl SyncClientHandler {
    async fn run(self, tcp_stream: TcpStream) -> miette::Result<()> {
        if let Err(e) = self.follower_serve_loop(tcp_stream).await {
            error!("Error in follower serve loop: {e}");
            eprintln!("{e:?}");
        }

        self.on_follower_disconnected
            .send_timeout(self.follower_addr, self.config.channel_buffer_timeout)
            .await
            .into_diagnostic()?;

        self.subsys
            .request_local_shutdown_because("TCP connection to follower/proxy closed");

        Ok(())
    }

    async fn follower_serve_loop(&self, tcp_stream: TcpStream) -> miette::Result<()> {
        let (send_tx, send_rx) = mpsc::channel(self.config.channel_buffer_size);
        let mut proxy_server = VirtualProxyServer {
            subsys: &self.subsys,
            clients: HashMap::new(),
            worterbuch: self.wb.named("server/virtual-proxy"),
            config: self.config.clone(),
            proxy_address: self.follower_addr,
            send_tx,
        };
        let mut proxy_id = None;

        let res = self
            .follower_session(tcp_stream, &mut proxy_server, send_rx, &mut proxy_id)
            .await;

        // no matter how the session ended, all of its clients must be disconnected before this task ends,
        // a new connection of the same proxy waits for this task to end before registering its clients
        proxy_server.disconnect_all().await;

        if let Some(proxy_id) = proxy_id {
            self.proxy_sessions
                .release_own(proxy_id, self.follower_addr);
        }

        res
    }

    async fn follower_session<'a>(
        &self,
        tcp_stream: TcpStream,
        proxy_server: &mut VirtualProxyServer<'a>,
        send_rx: mpsc::Receiver<VirtualServerMessage>,
        proxy_id: &mut Option<ProxyId>,
    ) -> miette::Result<()> {
        let (socket_rx, socket_tx) = tcp_stream.into_split();
        let mut socket_tx = BufWriter::new(socket_tx);
        let mut proxy_messages = BufReader::new(socket_rx).lines();

        self.send_welcome(&mut socket_tx).await?;
        let handshake = self.receive_handshake(&mut proxy_messages).await?;

        self.check_version(handshake.version())
            .wrap_err("version mismatch between leader and follower/proxy")?;

        let is_proxy = self
            .process_handshake(handshake, proxy_server, proxy_id)
            .await?;

        let (sync_tx, sync_rx) = oneshot::channel();
        self.on_follower_connected
            .send((sync_tx, self.follower_addr, is_proxy))
            .await
            .into_diagnostic()
            .wrap_err("failed to forward follower connected event")?;

        let (state, commands) = sync_rx.await.into_diagnostic()?;

        self.send_initial_state(
            proxy_server.subsys,
            &mut socket_tx,
            self.follower_addr,
            &self.config,
            state,
        )
        .await?;

        let follower_addr = self.follower_addr;
        let config = self.config.clone();

        let parent_subsys = proxy_server.subsys.clone();
        proxy_server.subsys.spawn(
            format!("state-and-response-forwarder/{}", follower_addr),
            move |s| async move {
                if let Err(e) = forward_messages_to_proxy(
                    s,
                    follower_addr,
                    config.clone(),
                    send_rx,
                    socket_tx,
                    commands,
                )
                .await
                {
                    error!("Error forwarding messages to proxy: {e}");
                    parent_subsys
                        .request_local_shutdown_because("state-and-response-forwarder failed");
                }
            },
        );

        while_select! {
            _ = proxy_server.subsys.shutdown_requested() => break,
            recv = proxy_messages.next_line() => proxy_server.process_proxy_message(recv, self.follower_addr).await.wrap_err("error processing proxy message")?,
        }

        info!(
            "TCP connection to follower/proxy {} closed.",
            self.follower_addr
        );

        Ok(())
    }

    async fn send_welcome(&self, tcp_stream: &mut BufWriter<OwnedWriteHalf>) -> miette::Result<()> {
        trace!(enter = "send_welcome");
        let authentication_required = self.config.auth_token_key.is_some();
        let version = worterbuch_version();

        write_line_and_flush(
            || self.subsys.shutdown_requested(),
            LeaderMessage::Welcome(LeaderWelcome {
                version,
                authentication_required,
            }),
            tcp_stream,
            self.config.send_timeout,
        )
        .await
        .into_diagnostic()
        .wrap_err_with(|| {
            format!(
                "could not send current state to follower/proxy {}",
                self.follower_addr
            )
        })?;
        trace!(exit = "send_welcome");
        Ok(())
    }

    async fn receive_handshake(
        &self,
        tcp_stream: &mut Lines<BufReader<OwnedReadHalf>>,
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
                            self.follower_addr,
                            msg
                        );
                    }
                }
            }
            Ok(None) => {
                trace!(exit = "receive_handshake", error = true);
                bail!(
                    "connection to follower/proxy {} closed before handshake",
                    self.follower_addr
                );
            }
            Err(e) => {
                trace!(exit = "receive_handshake", error = true);
                bail!(
                    "error receiving handshake message from follower/proxy {}: {}",
                    self.follower_addr,
                    e
                );
            }
        };
        trace!(exit = "receive_handshake");
        handshake
    }

    async fn process_handshake<'a>(
        &self,
        handshake: Handshake,
        proxy_server: &mut VirtualProxyServer<'a>,
        proxy_id: &mut Option<ProxyId>,
    ) -> miette::Result<bool> {
        trace!(enter = "process_handshake");

        match handshake {
            Handshake::Proxy(proxy_handshake) => {
                self.authenticate(proxy_handshake.auth_token.as_deref(), true)
                    .wrap_err("could not authenticate proxy")?;

                let id = proxy_handshake.proxy_id;
                *proxy_id = Some(id);
                self.take_over_proxy_session(id, proxy_server).await;

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
                        let _ = tx.send(msg).await;
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
                        let client = client.to_owned();
                        self.subsys.spawn("forward_lock_acquired", move |s| {
                            protocol::forward_lock_acquired(
                                s,
                                client,
                                transaction_id,
                                acquired_rx,
                                lost_rx,
                            )
                        });
                    }
                }

                trace!(exit = "process_handshake");
                Ok(true)
            }
            Handshake::Follower(follower_handshake) => {
                self.authenticate(follower_handshake.auth_token.as_deref(), false)
                    .wrap_err("could not authenticate follower")?;

                trace!(exit = "process_handshake");
                Ok(false)
            }
        }
    }

    /// Registers this connection as the active session of the proxy. If the proxy still has a previous connection
    /// (e.g. one that broke without the leader noticing yet), that connection is closed and all of its clients are
    /// disconnected before this returns, so that the clients of the new connection can be registered without
    /// colliding with their stale registrations.
    async fn take_over_proxy_session<'a>(
        &self,
        proxy_id: ProxyId,
        proxy_server: &VirtualProxyServer<'a>,
    ) {
        trace!(enter = "take_over_proxy_session");
        let previous = self.proxy_sessions.update(
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

    fn check_version(&self, version: &WorterbuchVersion) -> miette::Result<()> {
        ensure!(
            version == &worterbuch_version(),
            "follower/proxy version mismatch: expected {}, got {}",
            worterbuch_version(),
            version
        );
        Ok(())
    }

    fn authenticate(&self, auth_token: Option<&str>, is_proxy: bool) -> miette::Result<()> {
        trace!(enter = "authenticate");
        let claims = auth::get_claims(auth_token, &self.config)?;

        if is_proxy && !claims.is_proxy() {
            bail!("authentication failed: expected proxy token");
        }
        if !is_proxy && !claims.is_follower() {
            bail!("authentication failed: expected follower token");
        }

        trace!(exit = "authenticate");
        Ok(())
    }

    async fn send_initial_state(
        &self,
        subsys: &Subsystem,
        tcp_stream: &mut BufWriter<OwnedWriteHalf>,
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
}

async fn forward_messages_to_proxy(
    subsys: Subsystem,
    follower: SocketAddr,
    config: Config,
    mut send_rx: mpsc::Receiver<VirtualServerMessage>,
    mut socket_tx: BufWriter<OwnedWriteHalf>,
    mut commands: mpsc::Receiver<ClusterStateChange>,
) -> std::prelude::v1::Result<(), miette::Error> {
    while_select! {
        _ = subsys.shutdown_requested() => break,
        recv = commands.recv() => forward_change_to_follower(&subsys, recv, &mut socket_tx, &config, follower).await.wrap_err("error forwarding change message to follower/proxy")?,
        recv = send_rx.recv() => forward_response_to_proxy(&subsys, recv, &mut socket_tx, &config, follower).await.wrap_err("error forwarding response message to proxy")?,
    }
    Ok(())
}

async fn forward_change_to_follower(
    subsys: &Subsystem,
    recv: Option<ClusterStateChange>,
    socket_tx: &mut BufWriter<OwnedWriteHalf>,
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
    socket_tx: &mut BufWriter<OwnedWriteHalf>,
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
