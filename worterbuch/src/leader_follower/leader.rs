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
    error::WorterbuchAppResult,
    forward_api_call, forward_to_followers,
    leader_follower::{
        ClientWriteCommand, LeaderSyncMessage, Mode, StateSync, shutdown_on_stdin_close,
    },
    process_api_call,
    server::common::WbFunction,
    shutdown,
};
use miette::{Error, IntoDiagnostic, Result};
use serde_json::json;
use std::{
    io::{self, ErrorKind},
    net::{IpAddr, SocketAddr},
    ops::ControlFlow,
};
use tokio::{
    net::{TcpSocket, TcpStream},
    select,
    sync::{mpsc, oneshot},
};
use tosub::SubsystemHandle;
use tracing::{Level, debug, error, info, span};
use worterbuch_common::{
    KeySegment, PStateEvent, SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_GRAVE_GOODS,
    SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, ValueEntry, topic, while_select,
    write_line_and_flush,
};

pub(crate) async fn run_in_leader_mode(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    web_server: Option<SubsystemHandle>,
    tcp_server: Option<SubsystemHandle>,
    unix_socket: Option<SubsystemHandle>,
) -> WorterbuchAppResult<()> {
    info!("Running in LEADER mode.");

    shutdown_on_stdin_close(subsys);

    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Leader),
            INTERNAL_CLIENT_ID,
            true,
        )
        .await?;

    let mut client_write_txs: Vec<(usize, mpsc::Sender<ClientWriteCommand>)> = vec![];
    let (follower_connected_tx, mut follower_connected_rx) = mpsc::channel::<
        oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    >(config.channel_buffer_size);

    let mut tx_id = 0;
    let mut dead = vec![];

    let cfg = config.clone();
    subsys.spawn("cluster_sync_port", async move |s| {
        run_cluster_sync_port(s, cfg, follower_connected_tx).await
    });

    let (mut grave_goods_rx, _) = worterbuch
        .psubscribe(
            INTERNAL_CLIENT_ID,
            0,
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                KeySegment::Wildcard,
                SYSTEM_TOPIC_GRAVE_GOODS
            ),
            true,
            false,
        )
        .await?;
    let (mut last_will_rx, _) = worterbuch
        .psubscribe(
            INTERNAL_CLIENT_ID,
            0,
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                KeySegment::Wildcard,
                SYSTEM_TOPIC_LAST_WILL
            ),
            true,
            false,
        )
        .await?;

    while_select! {
        recv = grave_goods_rx.recv() => try_forward_grave_goods_change(recv, &mut client_write_txs, &mut dead).await?,
        recv = last_will_rx.recv() => try_forward_last_will_change(recv, &mut client_write_txs, &mut dead).await?,
        recv = api_rx.recv() => try_forward_api_call(recv, &mut worterbuch, &mut client_write_txs, &mut dead).await?,
        recv = follower_connected_rx.recv() => try_forward_follower_connected(recv, &mut worterbuch,&mut client_write_txs, &config, &mut tx_id).await?,
        _ = subsys.shutdown_requested() => break,
    }

    shutdown(
        subsys,
        worterbuch,
        config,
        web_server,
        tcp_server,
        unix_socket,
    )
    .await
}

async fn try_forward_grave_goods_change(
    recv: Option<PStateEvent>,
    client_write_txs: &mut Vec<(usize, mpsc::Sender<ClientWriteCommand>)>,
    dead: &mut Vec<usize>,
) -> WorterbuchAppResult<ControlFlow<()>> {
    if let Some(e) = recv {
        debug!("Forwarding grave goods change: {e:?}");
        match e {
            PStateEvent::KeyValuePairs(kvps) => {
                for kvp in kvps {
                    let span = span!(Level::DEBUG, "forward_grave_goods");
                    forward_api_call(
                        client_write_txs,
                        dead,
                        &WbFunction::Set(
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
                        &WbFunction::Delete(kvp.key, INTERNAL_CLIENT_ID, oneshot::channel().0),
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
    recv: Option<PStateEvent>,
    client_write_txs: &mut Vec<(usize, mpsc::Sender<ClientWriteCommand>)>,
    dead: &mut Vec<usize>,
) -> WorterbuchAppResult<ControlFlow<()>> {
    if let Some(e) = recv {
        debug!("Forwarding last will change: {e:?}");
        match e {
            PStateEvent::KeyValuePairs(kvps) => {
                for kvp in kvps {
                    let span = span!(Level::DEBUG, "forward_last_will");
                    forward_api_call(
                        client_write_txs,
                        dead,
                        &WbFunction::Set(
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
                        &WbFunction::Delete(kvp.key, INTERNAL_CLIENT_ID, oneshot::channel().0),
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
    client_write_txs: &mut Vec<(usize, mpsc::Sender<ClientWriteCommand>)>,
    dead: &mut Vec<usize>,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Some(WbFunction::Import(json, tx)) => {
            let (tx_int, rx_int) = oneshot::channel();
            process_api_call(worterbuch, WbFunction::Import(json, tx_int)).await;
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
                    forward_to_followers(cmd, client_write_txs, dead).await;
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
    recv: Option<oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>>,
    worterbuch: &mut Worterbuch,
    client_write_txs: &mut Vec<(usize, mpsc::Sender<ClientWriteCommand>)>,
    config: &Config,
    tx_id: &mut usize,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Some(state_tx) => {
            let (client_write_tx, client_write_rx) = mpsc::channel(config.channel_buffer_size);
            let (current_state, grave_goods, last_will) = worterbuch.export();
            if state_tx
                .send((
                    StateSync(current_state, grave_goods, last_will),
                    client_write_rx,
                ))
                .is_ok()
            {
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
    on_follower_connected: mpsc::Sender<
        oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    >,
) -> Result<()> {
    let port = config.sync_port.expect("no cluster sync port configured");
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

    socket.set_reuseaddr(true).into_diagnostic()?;
    #[cfg(target_family = "unix")]
    socket.set_reuseport(true).into_diagnostic()?;
    socket.bind(SocketAddr::new(ip, port)).into_diagnostic()?;

    let listener = socket.listen(1024).into_diagnostic()?;

    while_select! {
        client = listener.accept() => accecpt_client(client, &subsys, &config ,&on_follower_connected).await,
        _ = subsys.shutdown_requested() => break,
    }

    drop(listener);

    info!("Cluster sync port closed.");

    Ok(())
}

async fn accecpt_client(
    client: io::Result<(TcpStream, SocketAddr)>,
    subsys: &SubsystemHandle,
    config: &Config,
    on_follower_connected: &mpsc::Sender<
        oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    >,
) -> ControlFlow<()> {
    match client {
        Ok(client) => {
            // TODO reject connections from clients that are not cluster peers
            serve(subsys, client, on_follower_connected, config.clone()).await;
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
    on_follower_connected: &mpsc::Sender<
        oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    >,
    config: Config,
) {
    info!("Follower {} connected.", client.1);
    let (sync_tx, sync_rx) = oneshot::channel();
    if on_follower_connected.send(sync_tx).await.is_err() {
        return;
    }

    subsys.spawn(client.1.to_string(), async move |s| {
        forward_events_to_follower(s, client.0, client.1, sync_rx, config).await;
        Ok::<(), Error>(())
    });
}

async fn forward_events_to_follower(
    subsys: SubsystemHandle,
    mut tcp_stream: TcpStream,
    follower: SocketAddr,
    sync_rx: oneshot::Receiver<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    config: Config,
) {
    let (state, mut commands) = match sync_rx.await {
        Ok(it) => it,
        Err(_) => return,
    };

    if let Err(e) = write_line_and_flush(
        LeaderSyncMessage::Init(state),
        &mut tcp_stream,
        config.send_timeout,
        follower,
    )
    .await
    {
        error!("Could not send current state to follower: {e}");
        return;
    }

    let mut buf = [0u8; 1024];

    loop {
        select! {
            recv = commands.recv() => match recv {
                Some(cmd) => if let Err(e) = write_line_and_flush(LeaderSyncMessage::Mut(cmd), &mut tcp_stream, config.send_timeout, follower).await {
                    error!("Could not write command to follower: {e}");
                    break;
                },
                None => break,
            },
            read = tcp_stream.readable() => {
                if let Err(e) = read {
                    error!("Follower {follower} closed the connection: {e}");
                    break;
                }
                match tcp_stream.try_read(&mut buf) {
                    Ok(0) => {
                        info!("Follower {follower} closed the connection.");
                        break;
                    }
                    Err(e) => {
                        if e.kind() != ErrorKind::WouldBlock {
                            error!("Follower {follower} closed the connection: {e}");
                            break;
                        }
                    }
                    Ok(_) => {
                        // follower actually wrote womething, but we don't care
                    }
                }
            },
            _ = subsys.shutdown_requested() => break,
        }
    }

    drop(tcp_stream);

    info!("TCP connection to follower {} closed.", follower);
}
