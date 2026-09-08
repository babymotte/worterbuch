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

use crate::{
    Config, INTERNAL_CLIENT_ID, Worterbuch,
    auth::JwtClaims,
    cluster::{
        ClusterStateChangeReceiver, ClusterStateChangeSender, Mode, Servers, process_api_call,
        protocol::{
            ClusterStateChange, Connected, Disconnected, Handshake, LeaderMessage, LeaderWelcome,
            Locks, ProxyMessage, Request, StateSync,
        },
        shutdown,
    },
    error::WorterbuchAppResult,
    server::common::{
        self, CloneableWbApi, WbFunction,
        protocol::{self, Proto, ServerMessageBroadcaster},
    },
    worterbuch_version,
};
use hashbrown::HashMap;
use miette::{Context, Error, IntoDiagnostic, Result, miette};
use serde_json::json;
use std::{
    io::{self},
    net::{IpAddr, SocketAddr},
    ops::ControlFlow,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    net::{
        TcpSocket, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select, spawn,
    sync::{mpsc, oneshot},
};
use tosub::SubsystemHandle;
use totils::while_select;
use tracing::{Level, debug, enabled, error, info, trace, warn};
use worterbuch_common::{
    ClientId, Protocol, WbApi, WorterbuchVersion,
    protocol::v1::{
        ClientMessage, Err, ErrorCode, GraveGoods, Interface, InternalAction, LastWill,
        SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_MODE,
        SYSTEM_TOPIC_ROOT, ServerMessage, Trace,
    },
    topic, write_line_and_flush,
};

pub(crate) async fn run(
    subsys: &SubsystemHandle,
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
    match recv {
        Some(function) => {
            process_api_call(worterbuch, function).await;
        }
        None => return Ok(ControlFlow::Break(())),
    }
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
    match recv {
        Some((state_tx, remote_addr, is_proxy)) => {
            let (client_write_tx, client_write_rx) = mpsc::channel(config.channel_buffer_size);
            let (current_state, grave_goods, last_wills) = worterbuch.export();
            let state_sync = StateSync {
                store: current_state,
                lost_locks: Default::default(),
                grave_goods,
                last_wills,
            };
            if state_tx.send((state_sync, client_write_rx)).is_ok() {
                client_write_txs.push((*tx_id, client_write_tx.clone(), is_proxy));
                *tx_id += 1;
            }
            worterbuch.follower_connected(remote_addr, client_write_tx, is_proxy);
            Ok(ControlFlow::Continue(()))
        }
        None => Ok(ControlFlow::Break(())),
    }
}

async fn try_forward_follower_disconnected(
    recv: Option<SocketAddr>,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Some(remote_addr) => {
            worterbuch.follower_disconnected(remote_addr);
            Ok(ControlFlow::Continue(()))
        }
        None => Ok(ControlFlow::Break(())),
    }
}

async fn run_cluster_sync_port(
    subsys: SubsystemHandle,
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

    let socket = match ip {
        IpAddr::V4(_) => TcpSocket::new_v4().into_diagnostic()?,
        IpAddr::V6(_) => TcpSocket::new_v6().into_diagnostic()?,
    };

    // TODO set TCP timeout
    socket.set_reuseaddr(true).into_diagnostic()?;
    #[cfg(target_family = "unix")]
    socket.set_reuseport(true).into_diagnostic()?;
    socket.bind(SocketAddr::new(ip, port)).into_diagnostic()?;

    let listener = socket.listen(1024).into_diagnostic()?;

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        client = listener.accept() => accecpt_client(client, &subsys, &config, &wb, on_follower_connected.clone(), on_follower_disconnected.clone()).await,
    }

    drop(listener);

    info!("Cluster sync port closed.");

    Ok(())
}

async fn accecpt_client(
    client: io::Result<(TcpStream, SocketAddr)>,
    subsys: &SubsystemHandle,
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
    subsys: &SubsystemHandle,
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
        if let Err(e) = follower_serve_loop(
            s,
            client.0,
            client.1,
            on_follower_connected,
            on_follower_disconnected,
            config,
            wb,
        )
        .await
        {
            error!("Error in follower serve loop: {e}");
            eprintln!("{e:?}");
        }
        Ok::<(), Error>(())
    });
}

async fn follower_serve_loop(
    subsys: SubsystemHandle,
    tcp_stream: TcpStream,
    follower: SocketAddr,
    on_follower_connected: mpsc::Sender<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    on_follower_disconnected: mpsc::Sender<SocketAddr>,
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
        biased;
        _ = proxy_server.subsys.shutdown_requested() => break,
        recv = commands.recv() => forward_change_to_follower(&proxy_server.subsys, recv, &mut socket_tx, &config, follower).await.wrap_err("error forwarding change message to follower/proxy")?,
        recv = proxy_messages.next_line() => proxy_server.process_proxy_message(recv, follower).await.wrap_err("error processing proxy message")?,
        recv = send_rx.recv() => forward_response_to_proxy(&proxy_server.subsys, recv, &mut socket_tx, &config, follower).await.wrap_err("error forwarding response message to proxy")?,
    }

    info!("TCP connection to follower/proxy {} closed.", follower);

    on_follower_disconnected
        .send(follower)
        .await
        .into_diagnostic()
        .wrap_err("failed to forward follower disconnected event")?;

    Ok(())
}

async fn send_welcome(
    subsys: &SubsystemHandle,
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
        follower,
    )
    .await
    .into_diagnostic()
    .wrap_err("could not send current state to follower/proxy")
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
                msg => Err(miette!(
                    "expected handshake message from follower/proxy {}, but got:\n{:?}",
                    follower,
                    msg
                )),
            }
        }
        Ok(None) => Err(miette!(
            "connection to follower/proxy {} closed before handshake",
            follower
        )),
        Err(e) => Err(miette!(
            "error receiving handshake message from follower/proxy {}: {}",
            follower,
            e
        )),
    }
}

async fn process_handshake(
    handshake: Handshake,
    config: &Config,
    proxy_server: &mut VirtualProxyServer,
) -> miette::Result<bool> {
    check_version(handshake.version(), config)
        .wrap_err("could not check version of follower/proxy")?;

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
                tx.send(ServerMessage::Err(Err {
                    transaction_id: *tid,
                    error_code: ErrorCode::LockLost,
                    metadata: json!("lock lost").to_string(),
                }))
                .await
                .ok();
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
                spawn(protocol::forward_lock_events(
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

fn check_version(version: &WorterbuchVersion, config: &Config) -> miette::Result<()> {
    if version != &worterbuch_version() {
        return Err(miette!(
            "follower/proxy version mismatch: expected {}, got {}",
            worterbuch_version(),
            version
        ));
    }

    Ok(())
}

fn authenticate(auth_token: Option<&str>, config: &Config) -> miette::Result<()> {
    match &config.auth_token_key {
        Some(key) => authenticate_against_key(auth_token, key),
        None => Ok(()),
    }
}

fn authenticate_against_key(auth_token: Option<&str>, key: &str) -> miette::Result<()> {
    todo!()
}

async fn send_initial_state(
    subsys: &SubsystemHandle,
    tcp_stream: &mut OwnedWriteHalf,
    follower: SocketAddr,
    config: &Config,
    state: StateSync,
) -> miette::Result<()> {
    select! {
        biased;
        _ = subsys.shutdown_requested() => {
            warn!("Shutdown requested before initial sync completed.");
            Err(miette!("shut down before initial sync"))
        },
        res = write_line_and_flush(|| subsys.shutdown_requested(), LeaderMessage::Init(state), tcp_stream, config.send_timeout, follower) => {
            res.into_diagnostic().wrap_err("could not send current state to follower/proxy")
        }
    }
}

async fn forward_change_to_follower(
    subsys: &SubsystemHandle,
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
                follower,
            )
            .await
            .wrap_err("could not write command to follower/proxy")?;
        }
        None => return Ok(ControlFlow::Break(())),
    }

    Ok(ControlFlow::Continue(()))
}

async fn forward_response_to_proxy(
    subsys: &SubsystemHandle,
    recv: Option<(ClientId, ServerMessage)>,
    socket_tx: &mut OwnedWriteHalf,
    config: &Config,
    follower: SocketAddr,
) -> miette::Result<ControlFlow<()>> {
    match recv {
        Some((client_id, server_message)) => {
            write_line_and_flush(
                || subsys.shutdown_requested(),
                LeaderMessage::ClientResponse(client_id, server_message),
                socket_tx,
                config.send_timeout,
                follower,
            )
            .await
            .wrap_err("could not write command to follower/proxy")?;
        }
        None => return Ok(ControlFlow::Break(())),
    }

    Ok(ControlFlow::Continue(()))
}

struct VirtualProxyClientHandler {
    authorized: Option<JwtClaims>,
    proto: Proto,
}

struct VirtualProxyServer {
    subsys: SubsystemHandle,
    clients: HashMap<ClientId, VirtualProxyClientHandler>,
    worterbuch: CloneableWbApi,
    config: Config,
    proxy_address: SocketAddr,
    send_tx: mpsc::Sender<(ClientId, ServerMessage)>,
}

impl VirtualProxyServer {
    async fn process_proxy_message(
        &mut self,
        recv: io::Result<Option<String>>,
        follower: SocketAddr,
    ) -> miette::Result<ControlFlow<()>> {
        match recv
            .into_diagnostic()
            .wrap_err_with(|| format!("follower/proxy {follower} closed the connection"))?
        {
            Some(line) => {
                debug!("Received message from proxy: {line}");

                self.process_line(line)
                    .await
                    .wrap_err("could not process proxy message")?;

                Ok(ControlFlow::Continue(()))
            }
            None => {
                info!("Follower/proxy {follower} closed the connection.");
                Ok(ControlFlow::Break(()))
            }
        }
    }

    async fn process_line(&mut self, line: String) -> miette::Result<()> {
        trace!("Processing incoming message …");
        let msg: ProxyMessage = serde_json::from_str(&line)
            .into_diagnostic()
            .wrap_err("could not parse proxy message")?;

        match msg {
            ProxyMessage::Handshake(_) => {
                return Err(miette!(
                    "received handshake message from proxy after initial handshake"
                ));
            }
            ProxyMessage::Connected(Connected {
                client_id,
                protocol,
            }) => {
                self.spawn_virtual_client(
                    client_id,
                    protocol,
                    self.config.clone(),
                    self.worterbuch.named(format!("client/{client_id}")),
                )
                .await?;
            }
            ProxyMessage::Disconnected(Disconnected {
                client_id,
                protocol,
                grave_goods,
                last_will,
            }) => {
                self.apply_grave_goods_and_last_will(client_id, grave_goods, last_will)
                    .await?;
                self.stop_virtual_client(client_id, protocol).await?;
            }
            ProxyMessage::Request(Request {
                client_id,
                msg,
                interface,
            }) => {
                self.process_client_request(client_id, msg, interface)
                    .await?;
            }
        }

        Ok(())
    }

    async fn spawn_virtual_client(
        &mut self,
        client_id: ClientId,
        protocol: Protocol,
        config: Config,
        worterbuch: CloneableWbApi,
    ) -> miette::Result<ServerMessageBroadcaster> {
        let auth_required = config.auth_token_key.is_some();
        let (send_client_tx, send_client_rx) = mpsc::channel(config.channel_buffer_size);

        let send_tx = self.send_tx.clone();
        self.subsys.spawn("leader-response-forwarder", move |s| {
            response_forwarder_loop(s, send_client_rx, send_tx, client_id)
        });

        worterbuch
            .connected(
                client_id,
                None,
                Protocol::Proxied(Box::new(protocol.clone())),
            )
            .await
            .into_diagnostic()?;

        let proto = Proto::new(
            client_id,
            send_client_tx.clone(),
            auth_required,
            config,
            worterbuch,
        );

        let client = VirtualProxyClientHandler {
            authorized: None,
            proto,
        };

        self.clients.insert(client_id, client);

        info!(
            "New proxied client connected: {} ({}/{:?})",
            client_id, self.proxy_address, protocol
        );

        Ok(send_client_tx)
    }

    async fn stop_virtual_client(
        &mut self,
        client_id: ClientId,
        protocol: Protocol,
    ) -> miette::Result<()> {
        debug!("Stopping virtual client {client_id} …");
        if self.clients.remove(&client_id).is_none() {
            warn!(
                "Received disconnect for unknown client {client_id} ({}/{:?})",
                self.proxy_address, protocol
            );
            return Ok(());
        }

        debug!(
            "Virtual client {client_id} removed from local register, triggering client disconnect callback …"
        );

        self.worterbuch
            .disconnected(client_id, protocol.clone(), None)
            .await?;

        info!(
            "Proxied client disconnected: {} ({}/{:?})",
            client_id, self.proxy_address, protocol
        );

        Ok(())
    }

    async fn process_client_request(
        &mut self,
        client_id: ClientId,
        msg: ClientMessage,
        interface: Interface,
    ) -> miette::Result<()> {
        trace!("Processing incoming message …");
        let Some(client_handler) = self.clients.get_mut(&client_id) else {
            return Err(miette!(
                "Received request for unknown client {client_id} ({}/{:?})",
                self.proxy_address,
                interface
            ));
        };

        let msg_processed = client_handler
            .proto
            .process_client_message(msg, &mut client_handler.authorized)
            .await?;
        if !msg_processed {
            return Err(miette!(
                "Message processing failed for client {client_id} ({}/{:?})",
                self.proxy_address,
                interface
            ));
        }
        trace!("Processing incoming message done.");

        Ok(())
    }

    async fn register_clients(
        &mut self,
        connected_clients: &[Connected],
    ) -> miette::Result<HashMap<ClientId, ServerMessageBroadcaster>> {
        let mut client_txs = HashMap::new();

        for Connected {
            client_id,
            protocol,
        } in connected_clients
        {
            let tx = self
                .spawn_virtual_client(
                    *client_id,
                    protocol.clone(),
                    self.config.clone(),
                    self.worterbuch.named(format!("client/{client_id}")),
                )
                .await?;
            client_txs.insert(*client_id, tx);
        }

        Ok(client_txs)
    }

    async fn restore_locks(&self, locks: Locks) -> miette::Result<common::UpdatedLocks> {
        let updated_locks = self
            .worterbuch
            .re_grant_locks(locks.clone())
            .await
            .wrap_err("error while trying to re-grant previously held locks")?;

        for (client_id, keys) in &updated_locks.lost {
            if enabled!(Level::DEBUG) {
                let lost_keys: Vec<_> = keys.iter().map(|(_, key)| key).collect();
                debug!(
                    "Client {client_id} lost locks on keys {:?} after reconnecting to leader.",
                    lost_keys
                );
            }
        }

        for (client_id, keys) in &updated_locks.held {
            if enabled!(Level::DEBUG) {
                let held_keys: Vec<_> = keys.iter().map(|(_, key)| key).collect();
                debug!(
                    "Client {client_id} still holds locks on keys {:?} after reconnecting to leader.",
                    held_keys
                );
            }
        }

        for (client_id, pending_locks) in &updated_locks.pending {
            if enabled!(Level::DEBUG) {
                let pending_keys: Vec<_> = pending_locks.iter().map(|(_, key, _, _)| key).collect();
                debug!(
                    "Client {client_id} is still waiting for lock on keys {:?} after reconnecting to leader.",
                    pending_keys
                );
            }
        }

        Ok(updated_locks)
    }

    async fn apply_grave_goods_and_last_will(
        &self,
        client_id: ClientId,
        grave_goods: GraveGoods,
        last_will: LastWill,
    ) -> miette::Result<()> {
        self.worterbuch
            .set(
                0,
                topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_GRAVE_GOODS
                ),
                json!(grave_goods),
                client_id,
            )
            .await?;

        self.worterbuch
            .set(
                0,
                topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_LAST_WILL
                ),
                json!(last_will),
                client_id,
            )
            .await?;

        Ok(())
    }
}

async fn response_forwarder_loop(
    subsys: SubsystemHandle,
    mut send_client_rx: mpsc::Receiver<ServerMessage>,
    send_tx: mpsc::Sender<(ClientId, ServerMessage)>,
    client_id: uuid::Uuid,
) -> miette::Result<()> {
    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = send_client_rx.recv() => forward_leader_response(recv, &send_tx, client_id).await?,
    }

    Ok(())
}

async fn forward_leader_response(
    recv: Option<ServerMessage>,
    send_tx: &mpsc::Sender<(ClientId, ServerMessage)>,
    client_id: ClientId,
) -> miette::Result<ControlFlow<()>> {
    match recv {
        Some(msg) => {
            send_tx
                .send((client_id, msg))
                .await
                .into_diagnostic()
                .wrap_err("could not forward response to proxy")?;
        }
        None => return Ok(ControlFlow::Break(())),
    }

    Ok(ControlFlow::Continue(()))
}
