/*
 *  Helper functions for leader mode
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

mod virtual_proxy_server;

use crate::{
    Config, INTERNAL_CLIENT_ID, Worterbuch,
    cluster::{
        ClusterStateChangeReceiver, ClusterStateChangeSender, Mode, Servers,
        leader::virtual_proxy_server::{VirtualProxyServer, VirtualServerMessage},
        process_api_call,
        protocol::{
            ClusterStateChange, Handshake, LeaderMessage, LeaderWelcome, ProxyMessage, StateSync,
        },
        shutdown,
    },
    error::WorterbuchAppResult,
    server::common::{
        CloneableWbApi, WbFunction,
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
use tracing::{debug, error, info, trace};
use worterbuch_common::{
    WorterbuchVersion,
    protocol::v1::{
        Err, ErrorCode, InternalAction, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, ServerMessage, Trace,
    },
    socket::{TcpSocketConfig, create_tcp_server_socket},
    topic, write_line_and_flush,
};

pub(crate) async fn run(
    subsys: &Subsystem,
    mut worterbuch: Worterbuch,
    api: &CloneableWbApi,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    servers: Servers,
    sync_port: u16,
) -> WorterbuchAppResult<()> {
    #[cfg(feature = "commercial")]
    if !config.license.features.clustering {
        return Err(crate::error::WorterbuchAppError::NoLicense(
            "clustering".to_owned(),
        ));
    }

    info!("Running in LEADER mode.");

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Leader),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::Startup),
            true,
        )
        .await?;

    let mut client_write_txs: Vec<(usize, ClusterStateChangeSender, bool)> = vec![];
    let (follower_connected_tx, mut follower_connected_rx) = mpsc::channel::<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>(config.channel_buffer_size);
    let (follower_disconnected_tx, mut follower_disconnected_rx) =
        mpsc::channel::<SocketAddr>(config.channel_buffer_size);

    let mut tx_id = 0;
    // let mut dead = vec![];

    let cfg = config.clone();
    let wb = api.named("cluster-sync-port");
    subsys.spawn("cluster_sync_port", async move |s| {
        run_cluster_sync_port(
            s,
            cfg,
            wb,
            follower_connected_tx,
            follower_disconnected_tx,
            sync_port,
        )
        .await
    });

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        // recv = grave_goods_rx.recv() => try_forward_grave_goods_change(recv, &mut client_write_txs, &mut dead).await?,
        // recv = last_will_rx.recv() => try_forward_last_will_change(recv, &mut client_write_txs, &mut dead).await?,
        recv = follower_connected_rx.recv() => try_forward_follower_connected(recv, &mut worterbuch, &mut client_write_txs, &config, &mut tx_id).await?,
        recv = follower_disconnected_rx.recv() => try_forward_follower_disconnected(recv, &mut worterbuch).await?,
        recv = api_rx.recv() => try_forward_api_call(recv, &mut worterbuch).await?,
    }

    info!("Main loop stopped, shutting down.");

    shutdown(subsys, worterbuch, config, servers).await
}

async fn try_forward_api_call(
    recv: Option<WbFunction>,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<ControlFlow<()>> {
    // trace!(enter = "try_forward_api_call");
    match recv {
        Some(function) => {
            process_api_call(worterbuch, function).await;
        }
        None => {
            trace!(exit = "try_forward_api_call");
            return Ok(ControlFlow::Break(()));
        }
    }
    // trace!(exit = "try_forward_api_call");
    Ok(ControlFlow::Continue(()))
}

async fn try_forward_follower_connected(
    recv: Option<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    worterbuch: &mut Worterbuch,
    client_write_txs: &mut Vec<(usize, ClusterStateChangeSender, bool)>,
    config: &Config,
    tx_id: &mut usize,
) -> WorterbuchAppResult<ControlFlow<()>> {
    trace!(enter = "try_forward_follower_connected");
    match recv {
        Some((state_tx, remote_addr, is_proxy)) => {
            let (client_write_tx, client_write_rx) = mpsc::channel(config.channel_buffer_size);
            let (current_state, grave_goods, last_wills) = worterbuch.export();
            let state_sync = StateSync {
                store: current_state,
                grave_goods,
                last_wills,
            };
            if state_tx.send((state_sync, client_write_rx)).is_ok() {
                client_write_txs.push((*tx_id, client_write_tx.clone(), is_proxy));
                *tx_id += 1;
            }
            worterbuch.follower_connected(remote_addr, client_write_tx, is_proxy);
            trace!(exit = "try_forward_follower_connected");
            Ok(ControlFlow::Continue(()))
        }
        None => {
            trace!(exit = "try_forward_follower_connected");
            Ok(ControlFlow::Break(()))
        }
    }
}

async fn try_forward_follower_disconnected(
    recv: Option<SocketAddr>,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<ControlFlow<()>> {
    trace!(enter = "try_forward_follower_disconnected");
    match recv {
        Some(remote_addr) => {
            worterbuch.follower_disconnected(remote_addr);
            trace!(exit = "try_forward_follower_disconnected");
            Ok(ControlFlow::Continue(()))
        }
        None => {
            trace!(exit = "try_forward_follower_disconnected");
            Ok(ControlFlow::Break(()))
        }
    }
}

async fn run_cluster_sync_port(
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

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        client = socket.accept() => accecpt_client(client, &subsys, &config, &wb, on_follower_connected.clone(), on_follower_disconnected.clone()).await,
    }

    drop(socket);

    info!("Cluster sync port closed.");

    Ok(())
}

async fn accecpt_client(
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
) -> ControlFlow<()> {
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
            )
            .await;
            ControlFlow::Continue(())
        }
        Err(e) => {
            error!("Error accepting follower connections: {e}");
            ControlFlow::Break(())
        }
    }
}

async fn serve(
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
) {
    info!("Follower {} connected.", client.1);

    subsys.spawn(client.1.to_string(), async move |s| {
        let follower_addr = client.1;
        if let Err(e) =
            follower_serve_loop(&s, client.0, client.1, on_follower_connected, config, wb).await
        {
            error!("Error in follower serve loop: {e}");
            eprintln!("{e:?}");
        }

        on_follower_disconnected.send(follower_addr).await.ok();

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
) -> miette::Result<()> {
    let (socket_rx, mut socket_tx) = tcp_stream.into_split();
    let mut proxy_messages = BufReader::new(socket_rx).lines();

    let (send_tx, mut send_rx) = mpsc::channel(config.channel_buffer_size);
    let mut proxy_server = VirtualProxyServer {
        subsys: subsys.clone(),
        clients: HashMap::new(),
        worterbuch: worterbuch.named("server/virtual-proxy"),
        config: config.clone(),
        proxy_address: follower,
        send_tx,
    };

    send_welcome(&subsys, &mut socket_tx, follower, &config).await?;
    let handshake = receive_handshake(&mut proxy_messages, follower).await?;

    let is_proxy = process_handshake(handshake, &config, &mut proxy_server).await?;

    let (sync_tx, sync_rx) = oneshot::channel();
    on_follower_connected
        .send((sync_tx, follower, is_proxy))
        .await
        .into_diagnostic()
        .wrap_err("failed to forward follower connected event")?;

    let (state, mut commands) = sync_rx.await.into_diagnostic()?;

    send_initial_state(&subsys, &mut socket_tx, follower, &config, state).await?;

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
    })
}

async fn receive_handshake(
    tcp_stream: &mut Lines<BufReader<OwnedReadHalf>>,
    follower: SocketAddr,
) -> miette::Result<Handshake> {
    match tcp_stream.next_line().await {
        Ok(Some(line)) => {
            debug!("Received handshake message from follower/proxy: {line}");

            let msg: ProxyMessage = serde_json::from_str(&line)
                .into_diagnostic()
                .wrap_err("could not parse follower/proxy message")?;

            match msg {
                ProxyMessage::Handshake(handshake) => Ok(handshake),
                msg => bail!(
                    "expected handshake message from follower/proxy {}, but got:\n{:#?}",
                    follower,
                    msg
                ),
            }
        }
        Ok(None) => bail!(
            "connection to follower/proxy {} closed before handshake",
            follower
        ),
        Err(e) => bail!(
            "error receiving handshake message from follower/proxy {}: {}",
            follower,
            e
        ),
    }
}

async fn process_handshake(
    handshake: Handshake,
    config: &Config,
    proxy_server: &mut VirtualProxyServer,
) -> miette::Result<bool> {
    check_version(handshake.version())
        .wrap_err("version mismatch between leader and follower/proxy")?;

    authenticate(handshake.auth_token(), config)
        .wrap_err("could not authenticate follower/proxy")?;

    if let Handshake::Proxy(proxy_handshake) = handshake {
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
                tx.lazy_send(msg).await.ok();
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

        Ok(true)
    } else {
        Ok(false)
    }
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
    match &config.auth_token_key {
        Some(key) => authenticate_against_key(auth_token, key),
        None => Ok(()),
    }
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
    })
}

async fn forward_change_to_follower(
    subsys: &Subsystem,
    recv: Option<ClusterStateChange>,
    socket_tx: &mut OwnedWriteHalf,
    config: &Config,
    follower: SocketAddr,
) -> miette::Result<ControlFlow<()>> {
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
        None => return Ok(ControlFlow::Break(())),
    }

    Ok(ControlFlow::Continue(()))
}

async fn forward_response_to_proxy(
    subsys: &Subsystem,
    recv: Option<VirtualServerMessage>,
    socket_tx: &mut OwnedWriteHalf,
    config: &Config,
    follower: SocketAddr,
) -> miette::Result<ControlFlow<()>> {
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
        None => return Ok(ControlFlow::Break(())),
    }

    Ok(ControlFlow::Continue(()))
}
