/*
 *  Worterbuch server TCP module
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
    server::common::{
        check_client_keepalive, process_incoming_message, send_keepalive, CloneableWbApi,
    },
    stats::VERSION,
};
use anyhow::anyhow;
use std::{
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    select, spawn,
    sync::mpsc,
    time::{sleep, MissedTickBehavior},
};
use tokio_graceful_shutdown::SubsystemHandle;
use uuid::Uuid;
use worterbuch_common::{tcp::write_line_and_flush, Protocol, ServerInfo, ServerMessage, Welcome};

pub async fn start(
    worterbuch: CloneableWbApi,
    bind_addr: IpAddr,
    port: u16,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let addr = format!("{bind_addr}:{port}");

    log::info!("Serving TCP endpoint at {addr}");
    let listener = TcpListener::bind(&addr).await?;

    loop {
        select! {
            con = listener.accept() => {
                let (socket, remote_addr) = con?;
                let worterbuch = worterbuch.clone();
                spawn(async move {
                    if let Err(e) = serve(remote_addr, worterbuch, socket).await {
                        log::error!("Connection to client {remote_addr} closed with error: {e}");
                    }
                });
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}

async fn serve(
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    worterbuch
        .connected(client_id, remote_addr, Protocol::TCP)
        .await?;

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

    if let Err(e) = serve_loop(client_id, remote_addr, worterbuch.clone(), socket).await {
        log::error!("Error in serve loop: {e}");
    }

    worterbuch.disconnected(client_id, remote_addr).await?;

    Ok(())
}

type TcpSender = OwnedWriteHalf;

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
) -> anyhow::Result<()> {
    let config = worterbuch.config().await?;
    let authorization_required = config.auth_token.is_some();
    let send_timeout = config.send_timeout;
    let keepalive_timeout = config.keepalive_timeout;
    let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
    let mut last_keepalive_tx = Instant::now();
    let mut last_keepalive_rx = Instant::now();
    let mut authorized = None;
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let (tcp_rx, mut tcp_tx) = socket.into_split();
    let (tcp_send_tx, mut tcp_send_rx) = mpsc::channel(config.channel_buffer_size);
    let (keepalive_tx_tx, mut keepalive_tx_rx) = mpsc::channel(config.channel_buffer_size);

    // tcp socket send loop
    spawn(async move {
        while let Some(msg) = tcp_send_rx.recv().await {
            send_with_timeout(msg, &mut tcp_tx, send_timeout, &keepalive_tx_tx).await;
        }
    });

    let tcp_rx = BufReader::new(tcp_rx);
    let mut tcp_rx = tcp_rx.lines();

    let protocol_version = worterbuch.supported_protocol_version().await?;

    tcp_send_tx
        .send(ServerMessage::Welcome(Welcome {
            client_id: client_id.to_string(),
            info: ServerInfo {
                version: VERSION.to_owned(),
                authorization_required,
                protocol_version,
            },
        }))
        .await?;

    loop {
        select! {
            recv = tcp_rx.next_line() => match recv {
                Ok(Some(json)) => {
                    last_keepalive_rx = Instant::now();

                    // drain the send buffer to make room for the response
                        while let Ok(keepalive) = keepalive_tx_rx.try_recv() {
                            last_keepalive_tx = keepalive?;
                        }
                    log::trace!("Processing incoming message …");
                    let (msg_processed, auth) = process_incoming_message(
                        client_id,
                        &json,
                        &worterbuch,
                        &tcp_send_tx,
                        authorization_required,
                        authorized,
                        &config
                    ).await?;
                    authorized = auth;
                    if !msg_processed {
                        break;
                    }
                    log::trace!("Processing incoming message done.");
                },
                Ok(None) =>  break,
                Err(e) => {
                    log::info!("TCP stream of client {client_id} ({remote_addr}) closed with error:, {e}");
                    break;
                }
            } ,
            recv = keepalive_tx_rx.recv() => match recv {
                Some(keepalive) => {
                    last_keepalive_tx = keepalive?;
                    while let Ok(keepalive) = keepalive_tx_rx.try_recv() {
                        last_keepalive_tx = keepalive?;
                    }
                },
                None => break,
            },
            _ = keepalive_timer.tick() => {
                // check how long ago the last websocket message was received
                check_client_keepalive(last_keepalive_rx, last_keepalive_tx, client_id, keepalive_timeout)?;
                // send out websocket message if the last has been more than a second ago
                send_keepalive(last_keepalive_tx, &tcp_send_tx, ).await?;
            }
        }
    }

    Ok(())
}

async fn send_with_timeout<'a>(
    msg: ServerMessage,
    tcp: &mut TcpSender,
    send_timeout: Duration,
    keepalive_tx_tx: &mpsc::Sender<anyhow::Result<Instant>>,
) {
    log::trace!("Sending with timeout {}s …", send_timeout.as_secs());
    select! {
        r = write_line_and_flush(&msg, tcp)  => {
            if let Err(e) = r {
                log::trace!("Writing line produced an error, queuing keepalive error …");
                keepalive_tx_tx.send(Err(e.into())).await.ok();
                log::trace!("Writing line produced an error, queuing keepalive error done.");
            } else {
                keepalive_tx_tx.try_send(Ok(Instant::now())).ok();
            }
        },
        _ = sleep(send_timeout) => {
            log::error!("Send timeout");
            log::trace!("Writing line timed out, queuing keepalive error …");
            keepalive_tx_tx.send(Err(anyhow!("Send timeout"))).await.ok();
            log::trace!("Writing line timed out, queuing keepalive error done.");
        },
    }
    log::trace!("Sending with timeout {}s done.", send_timeout.as_secs());
}
