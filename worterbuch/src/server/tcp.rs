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
use std::net::{IpAddr, SocketAddr};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpSocket, TcpStream},
    select, spawn,
    sync::mpsc,
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
    let sock = TcpSocket::new_v4()?;
    sock.set_keepalive(true)?;
    sock.set_nodelay(true)?;
    #[cfg(not(target_os = "windows"))]
    sock.set_reuseaddr(true)?;
    sock.bind(addr.parse()?)?;
    let listener = sock.listen(1024)?;

    let (conn_closed_tx, mut conn_closed_rx) = mpsc::channel(100);
    let mut open_connections = 0;
    let mut waiting_for_free_connections = false;

    loop {
        select! {
            recv = conn_closed_rx.recv() => if recv.is_some() {
                open_connections -= 1;
                while conn_closed_rx.try_recv().is_ok() {
                    open_connections -= 1;
                }
                log::debug!("{open_connections} TCP connection(s) open.");
                waiting_for_free_connections = false;
            } else {
                break;
            },
            con = listener.accept(), if !waiting_for_free_connections => {
                log::debug!("Trying to accept new client connection.");
                match con {
                    Ok((socket, remote_addr)) => {
                        open_connections += 1;
                        log::debug!("{open_connections} TCP connection(s) open.");
                        let worterbuch = worterbuch.clone();
                        let conn_closed_tx = conn_closed_tx.clone();
                        spawn(async move {
                            if let Err(e) = serve(remote_addr, worterbuch, socket).await {
                                log::error!("Connection to client {remote_addr} closed with error: {e}");
                            }
                            conn_closed_tx.send(()).await.ok();
                        });
                    },
                    Err(e) => {
                        log::error!("Error while trying to accept client connection: {e}");
                        log::warn!("{open_connections} TCP connections open, waiting for connections to close.");
                        waiting_for_free_connections = true;
                    }
                }
                log::debug!("Ready to accept new connections.");
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    log::debug!("tcpserver subsystem completed.");

    Ok(())
}

async fn serve(
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    if let Err(e) = worterbuch
        .connected(client_id, Some(remote_addr), Protocol::TCP)
        .await
    {
        log::error!("Error while adding new client: {e}");
    } else {
        log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

        if let Err(e) = serve_loop(client_id, remote_addr, worterbuch.clone(), socket).await {
            log::error!("Error in serve loop: {e}");
        }
    }

    worterbuch
        .disconnected(client_id, Some(remote_addr))
        .await?;

    Ok(())
}

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
) -> anyhow::Result<()> {
    let config = worterbuch.config().await?;
    let authorization_required = config.auth_token.is_some();
    let send_timeout = config.send_timeout;
    let mut authorized = None;

    let (tcp_rx, mut tcp_tx) = socket.into_split();
    let (tcp_send_tx, mut tcp_send_rx) = mpsc::channel(config.channel_buffer_size);

    // tcp socket send loop
    spawn(async move {
        while let Some(msg) = tcp_send_rx.recv().await {
            if let Err(e) = write_line_and_flush(msg, &mut tcp_tx, send_timeout, client_id).await {
                log::error!("Error sending TCP message: {e}");
                break;
            }
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
                    log::warn!("TCP stream of client {client_id} ({remote_addr}) closed with error:, {e}");
                    break;
                }
            }
        }
    }

    Ok(())
}
