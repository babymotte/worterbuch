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
            ClientWriteCommand, ClusterStateChange, LeaderMessage, ProxyMessage, StateSync,
        },
        shutdown,
    },
    error::WorterbuchAppResult,
    forward_api_call, forward_to_followers,
    server::common::{CloneableWbApi, WbFunction, protocol::Proto},
    worterbuch::SubscriptionFlags,
};
use hashbrown::HashMap;
use miette::{Context, Error, IntoDiagnostic, Result, miette};
use serde_json::json;
use std::{
    io::{self},
    net::{IpAddr, SocketAddr},
    ops::ControlFlow,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpSocket, TcpStream, tcp::OwnedWriteHalf},
    select,
    sync::{mpsc, oneshot},
};
use tosub::SubsystemHandle;
use tracing::{Level, debug, error, info, span, trace, warn};
use worterbuch_common::{
    KeySegment, Protocol, ValueEntry,
    protocol::v1::{
        ClientId, ClientMessage, Interface, InternalAction, Method, PStateEvent,
        SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_MODE,
        SYSTEM_TOPIC_ROOT, ServerMessage, Trace,
    },
    topic, while_select, write_line_and_flush,
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

    let mut client_write_txs: Vec<(usize, ClusterStateChangeSender)> = vec![];
    let (follower_connected_tx, mut follower_connected_rx) = mpsc::channel::<
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
    >(config.channel_buffer_size);

    let mut tx_id = 0;
    let mut dead = vec![];

    let cfg = config.clone();
    let wb = api.named("cluster-sync-port");
    subsys.spawn("cluster_sync_port", async move |s| {
        run_cluster_sync_port(s, cfg, wb, follower_connected_tx, sync_port).await
    });

    let (mut grave_goods_rx, _) = worterbuch
        .internal_psubscribe(
            INTERNAL_CLIENT_ID,
            0,
            Trace::InternalAction(InternalAction::Startup),
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                KeySegment::Wildcard,
                SYSTEM_TOPIC_GRAVE_GOODS
            ),
            SubscriptionFlags::new(true, false, false),
        )
        .await?;
    let (mut last_will_rx, _) = worterbuch
        .internal_psubscribe(
            INTERNAL_CLIENT_ID,
            0,
            Trace::InternalAction(InternalAction::Startup),
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                KeySegment::Wildcard,
                SYSTEM_TOPIC_LAST_WILL
            ),
            SubscriptionFlags::new(true, false, false),
        )
        .await?;

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = grave_goods_rx.recv() => try_forward_grave_goods_change(recv, &mut client_write_txs, &mut dead).await?,
        recv = last_will_rx.recv() => try_forward_last_will_change(recv, &mut client_write_txs, &mut dead).await?,
        recv = follower_connected_rx.recv() => try_forward_follower_connected(recv, &mut worterbuch,&mut client_write_txs, &config, &mut tx_id).await?,
        recv = api_rx.recv() => try_forward_api_call(recv, &mut worterbuch, &mut client_write_txs, &mut dead).await?,
    }

    info!("Main loop stopped, shutting down.");

    shutdown(subsys, worterbuch, config, servers).await
}

async fn try_forward_grave_goods_change(
    recv: Option<(PStateEvent, Option<Trace>)>,
    client_write_txs: &mut Vec<(usize, ClusterStateChangeSender)>,
    dead: &mut Vec<usize>,
) -> WorterbuchAppResult<ControlFlow<()>> {
    if let Some((e, _)) = recv {
        debug!("Forwarding grave goods change: {e:?}");
        match e {
            PStateEvent::KeyValuePairs(kvps) => {
                for kvp in kvps {
                    let span = span!(Level::DEBUG, "forward_grave_goods");
                    forward_api_call(
                        client_write_txs,
                        dead,
                        &WbFunction::Set(
                            0,
                            Interface::Local,
                            kvp.key,
                            kvp.value,
                            INTERNAL_CLIENT_ID,
                            oneshot::channel().0,
                            span,
                        ),
                        false,
                    )
                    .await;
                }
            }
            PStateEvent::Deleted(kvps) => {
                for kvp in kvps {
                    forward_api_call(
                        client_write_txs,
                        dead,
                        &WbFunction::Delete(
                            0,
                            Interface::Local,
                            kvp.key,
                            INTERNAL_CLIENT_ID,
                            oneshot::channel().0,
                        ),
                        false,
                    )
                    .await;
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    } else {
        Ok(ControlFlow::Break(()))
    }
}

async fn try_forward_last_will_change(
    recv: Option<(PStateEvent, Option<Trace>)>,
    client_write_txs: &mut Vec<(usize, ClusterStateChangeSender)>,
    dead: &mut Vec<usize>,
) -> WorterbuchAppResult<ControlFlow<()>> {
    if let Some((e, _)) = recv {
        debug!("Forwarding last will change: {e:?}");
        match e {
            PStateEvent::KeyValuePairs(kvps) => {
                for kvp in kvps {
                    let span = span!(Level::DEBUG, "forward_last_will");
                    forward_api_call(
                        client_write_txs,
                        dead,
                        &WbFunction::Set(
                            0,
                            Interface::Local,
                            kvp.key,
                            kvp.value,
                            INTERNAL_CLIENT_ID,
                            oneshot::channel().0,
                            span,
                        ),
                        false,
                    )
                    .await;
                }
            }
            PStateEvent::Deleted(kvps) => {
                for kvp in kvps {
                    forward_api_call(
                        client_write_txs,
                        dead,
                        &WbFunction::Delete(
                            0,
                            Interface::Local,
                            kvp.key,
                            INTERNAL_CLIENT_ID,
                            oneshot::channel().0,
                        ),
                        false,
                    )
                    .await;
                }
            }
        }

        Ok(ControlFlow::Continue(()))
    } else {
        Ok(ControlFlow::Break(()))
    }
}

async fn try_forward_api_call(
    recv: Option<WbFunction>,
    worterbuch: &mut Worterbuch,
    client_write_txs: &mut Vec<(usize, ClusterStateChangeSender)>,
    dead: &mut Vec<usize>,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Some(WbFunction::Import(transaction_id, client_id, interface, json, tx)) => {
            let (tx_int, rx_int) = oneshot::channel();
            process_api_call(
                worterbuch,
                WbFunction::Import(transaction_id, client_id, interface.clone(), json, tx_int),
            )
            .await;
            let imported_values = rx_int.await??;

            for (key, (value, changed)) in &imported_values {
                if *changed {
                    let cmd = match value.to_owned() {
                        ValueEntry::Cas(value, version) => {
                            ClientWriteCommand::CSet(key.to_owned(), value, version, true)
                        }
                        ValueEntry::Plain(value) => {
                            ClientWriteCommand::Set(key.to_owned(), value, true)
                        }
                    };
                    let trace = Trace::ClientRequest {
                        client_id,
                        transaction_id,
                        method: Method::Import,
                        interface: interface.clone(),
                    };
                    forward_to_followers(cmd, client_id, trace, client_write_txs, dead).await;
                }
            }
            tx.send(Ok(imported_values)).ok();
        }
        Some(function) => {
            // TODO check if processing was successful and only then forward api call
            forward_api_call(client_write_txs, dead, &function, true).await;
            process_api_call(worterbuch, function).await;
        }
        None => return Ok(ControlFlow::Break(())),
    }
    Ok(ControlFlow::Continue(()))
}

async fn try_forward_follower_connected(
    recv: Option<oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>>,
    worterbuch: &mut Worterbuch,
    client_write_txs: &mut Vec<(usize, ClusterStateChangeSender)>,
    config: &Config,
    tx_id: &mut usize,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Some(state_tx) => {
            let (client_write_tx, client_write_rx) = mpsc::channel(config.channel_buffer_size);
            let (current_state, locks, grave_goods, last_will) = worterbuch.export_with_locks();
            let state_sync = StateSync {
                store: current_state,
                locks,
                grave_goods,
                last_will,
            };
            if state_tx.send((state_sync, client_write_rx)).is_ok() {
                client_write_txs.push((*tx_id, client_write_tx));
                *tx_id += 1;
            }
            Ok(ControlFlow::Continue(()))
        }
        None => Ok(ControlFlow::Break(())),
    }
}

async fn run_cluster_sync_port(
    subsys: SubsystemHandle,
    config: Config,
    wb: CloneableWbApi,
    on_follower_connected: mpsc::Sender<oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>>,
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
        client = listener.accept() => accecpt_client(client, &subsys, &config, &wb ,&on_follower_connected).await,
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
    on_follower_connected: &mpsc::Sender<oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>>,
) -> ControlFlow<()> {
    match client {
        Ok(client) => {
            // TODO reject connections from clients that are not cluster peers or have proper proxy authentication
            let name = format!("follower-proxy/{}", client.1);
            serve(
                subsys,
                client,
                on_follower_connected,
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
    on_follower_connected: &mpsc::Sender<oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>>,
    config: Config,
    wb: CloneableWbApi,
) {
    info!("Follower {} connected.", client.1);
    let (sync_tx, sync_rx) = oneshot::channel();
    if on_follower_connected.send(sync_tx).await.is_err() {
        return;
    }

    subsys.spawn(client.1.to_string(), async move |s| {
        if let Err(e) = follower_serve_loop(s, client.0, client.1, sync_rx, config, wb).await {
            error!("Error in follower serve loop: {e}");
            eprintln!("{e:?}");
        }
        Ok::<(), Error>(())
    });
}

async fn follower_serve_loop(
    subsys: SubsystemHandle,
    mut tcp_stream: TcpStream,
    follower: SocketAddr,
    sync_rx: oneshot::Receiver<(StateSync, ClusterStateChangeReceiver)>,
    config: Config,
    worterbuch: CloneableWbApi,
) -> miette::Result<()> {
    let (state, mut commands) = sync_rx.await.into_diagnostic()?;

    if let Err(e) = write_line_and_flush(
        LeaderMessage::Init(state),
        &mut tcp_stream,
        config.send_timeout,
        follower,
    )
    .await
    {
        return Err(miette!(
            "Could not send current state to follower/proxy: {e}"
        ));
    }

    let (socket_rx, mut socket_tx) = tcp_stream.into_split();
    let mut proxy_messages = BufReader::new(socket_rx).lines();
    let mut proxy_server = VirtualProxyServer {
        subsys,
        clients: HashMap::new(),
        worterbuch: worterbuch.named("server/virtual-proxy"),
        config: config.clone(),
        proxy_address: follower,
    };

    while_select! {
        biased;
        _ = proxy_server.subsys.shutdown_requested() => break,
        recv = commands.recv() => forward_to_follower(recv, &mut socket_tx, &config, follower).await.wrap_err("error forwarding message to follower/proxy")?,
        recv = proxy_messages.next_line() => proxy_server.process_proxy_message(recv, follower).await.wrap_err("error processing proxy message")?,
    }

    info!("TCP connection to follower/proxy {} closed.", follower);

    Ok(())
}

async fn forward_to_follower(
    recv: Option<ClusterStateChange>,
    socket_tx: &mut OwnedWriteHalf,
    config: &Config,
    follower: SocketAddr,
) -> miette::Result<ControlFlow<()>> {
    match recv {
        Some(change) => {
            write_line_and_flush(
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

struct VirtualProxyClientHandler {
    client_id: ClientId,
    authorized: Option<JwtClaims>,
    proto: Proto,
    interface: Interface,
}

struct VirtualProxyServer {
    subsys: SubsystemHandle,
    clients: HashMap<ClientId, VirtualProxyClientHandler>,
    worterbuch: CloneableWbApi,
    config: Config,
    proxy_address: SocketAddr,
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
            ProxyMessage::Connected {
                client_id,
                protocol,
            } => self.spawn_virtual_client(
                client_id,
                protocol,
                self.config.clone(),
                self.worterbuch.named(format!("client/{client_id}")),
            ),
            ProxyMessage::Disconnected {
                client_id,
                protocol,
            } => self.stop_virtual_client(client_id, protocol),
            ProxyMessage::ProtocolSwitched {
                client_id,
                interface,
                version,
            } => self.switch_client_protocol(client_id, interface, version),
            ProxyMessage::Request {
                client_id,
                msg,
                interface,
            } => {
                self.process_client_request(client_id, msg, interface)
                    .await?
            }
        }

        Ok(())
    }

    fn spawn_virtual_client(
        &mut self,
        client_id: ClientId,
        protocol: Protocol,
        config: Config,
        worterbuch: CloneableWbApi,
    ) {
        let (send_tx, send_rx) = mpsc::channel(config.channel_buffer_size);
        let send_timeout = config.send_timeout;
        self.subsys
            .spawn(format!("message-forwarder/{client_id}"), async move |s| {
                forward_messages_to_socket(
                    s,
                    send_rx,
                    // tcp_tx,
                    client_id,
                    send_timeout,
                )
                .await
            });
        let auth_required = config.auth_token_key.is_some();
        let proto = Proto::new(client_id, send_tx, auth_required, config, worterbuch);

        let client = VirtualProxyClientHandler {
            client_id,
            authorized: None,
            proto,
            interface: Interface::Protocol(Protocol::Proxied(Box::new(protocol.clone()))),
        };

        self.clients.insert(client_id, client);

        info!(
            "New proxied client connected: {} ({}/{:?})",
            client_id, self.proxy_address, protocol
        );
    }

    fn stop_virtual_client(&mut self, client_id: ClientId, protocol: Protocol) {
        if self.clients.remove(&client_id).is_none() {
            warn!(
                "Received disconnect for unknown client {client_id} ({}/{:?})",
                self.proxy_address, protocol
            );
            return;
        }

        info!(
            "Proxied client disconnected: {} ({}/{:?})",
            client_id, self.proxy_address, protocol
        );
    }

    fn switch_client_protocol(&self, client_id: ClientId, interface: Interface, version: u32) {
        // TODO
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
}

async fn forward_messages_to_socket(
    subsys: SubsystemHandle,
    mut tcp_send_rx: mpsc::Receiver<ServerMessage>,
    // mut tcp_tx: OwnedWriteHalf,
    client_id: ClientId,
    send_timeout: Option<Duration>,
) -> Result<()> {
    loop {
        select! {
            recv = tcp_send_rx.recv() => if let Some(msg) = recv {
                // if let Err(e) = write_line_and_flush(&msg, &mut tcp_tx, send_timeout, client_id).await {
                //     error!("Error sending TCP message '{msg:?}': {e}");
                //     break;
                // }
                eprint!("//TODO: forward {:?}", msg);
            } else {
                debug!("Message forwarding to client {client_id} stopped: channel closed.");
                break;
            },
            _ = subsys.shutdown_requested() => {
                debug!("Message forwarding to client {client_id} stopped: subsystem stopped.");
                break;
            },
        }
    }

    Ok(())
}
