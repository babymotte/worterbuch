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
    server::common::{process_incoming_message, CloneableWbApi},
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
    let auth_token = config.auth_token;
    let authentication_required = auth_token.is_some();
    let send_timeout = config.send_timeout;
    let keepalive_timeout = config.keepalive_timeout;
    let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
    let mut last_keepalive_tx = Instant::now();
    let mut last_keepalive_rx = Instant::now();
    let mut already_authenticated = false;
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let (tcp_rx, mut tcp_tx) = socket.into_split();
    let (tcp_send_tx, mut tcp_send_rx) = mpsc::channel(config.channel_buffer_size);
    let (keepalive_tx_tx, mut keepalive_tx_rx) = mpsc::unbounded_channel();

    // websocket send loop
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
                authentication_required,
                protocol_version,
            },
        }))
        .await?;

    loop {
        select! {
            recv = tcp_rx.next_line() => match recv {
                Ok(Some(json)) => {
                    last_keepalive_rx = Instant::now();
                    let res = process_incoming_message(
                        client_id,
                        &json,
                        &worterbuch,
                        &tcp_send_tx,
                        authentication_required,
                        already_authenticated
                    ).await;
                    let (msg_processed, authenticated) = res?;
                    already_authenticated |= msg_processed && authenticated;
                    if !msg_processed {
                        break;
                    }
                },
                Ok(None) =>  break,
                Err(e) => {
                    log::info!("TCP stream of client {client_id} ({remote_addr}) closed with error:, {e}");
                    break;
                }
            } ,
            recv = keepalive_tx_rx.recv() => match recv {
                Some(keepalive) => last_keepalive_tx = keepalive?,
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

async fn send_keepalive(
    last_keepalive_tx: Instant,
    ws_send_tx: &mpsc::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    if last_keepalive_tx.elapsed().as_secs() >= 1 {
        log::trace!("Sending keepalive");
        ws_send_tx.send(ServerMessage::Keepalive).await?;
    }
    Ok(())
}

fn check_client_keepalive(
    last_keepalive_rx: Instant,
    last_keepalive_tx: Instant,
    client_id: Uuid,
    keepalive_timeout: Duration,
) -> anyhow::Result<()> {
    let lag = last_keepalive_tx - last_keepalive_rx;

    if lag >= Duration::from_secs(2) {
        log::warn!(
            "Client {} has been inactive for {} seconds …",
            client_id,
            lag.as_secs()
        );
    }

    if lag >= keepalive_timeout {
        log::warn!(
            "Client {} has been inactive for too long. Disconnecting.",
            client_id
        );
        Err(anyhow!("Client has been inactive for too long"))
    } else {
        Ok(())
    }
}

async fn send_with_timeout<'a>(
    msg: ServerMessage,
    tcp: &mut TcpSender,
    send_timeout: Duration,
    result_handler: &mpsc::UnboundedSender<anyhow::Result<Instant>>,
) {
    select! {
        r = write_line_and_flush(&msg, tcp)  => {
            if let Err(e) = r {
                result_handler.send(Err(e.into())).ok();
            } else {
                result_handler.send(Ok(Instant::now())).ok();
            }
        },
        _ = sleep(send_timeout) => {
            log::error!("Send timeout");
            result_handler.send(Err(anyhow!("Send timeout"))).ok();
        },
    }
}
