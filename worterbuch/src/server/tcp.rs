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

use super::common::protocol::Proto;
use crate::{SUPPORTED_PROTOCOL_VERSIONS, server::common::CloneableWbApi, stats::VERSION};
use miette::{IntoDiagnostic, Result};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpSocket, TcpStream},
    select, spawn,
    sync::mpsc,
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use worterbuch_common::{Protocol, ServerInfo, ServerMessage, Welcome, tcp::write_line_and_flush};

pub async fn start(
    worterbuch: CloneableWbApi,
    bind_addr: IpAddr,
    port: u16,
    subsys: SubsystemHandle,
) -> Result<()> {
    let addr = format!("{bind_addr}:{port}");

    info!("Serving TCP endpoint at {addr}");
    let sock = TcpSocket::new_v4().into_diagnostic()?;
    sock.set_keepalive(true).into_diagnostic()?;
    sock.set_nodelay(true).into_diagnostic()?;
    #[cfg(not(target_os = "windows"))]
    sock.set_reuseaddr(true).into_diagnostic()?;
    sock.bind(addr.parse().into_diagnostic()?)
        .into_diagnostic()?;
    let listener = sock.listen(1024).into_diagnostic()?;

    let (conn_closed_tx, mut conn_closed_rx) = mpsc::channel(100);
    let mut waiting_for_free_connections = false;

    let mut clients = HashMap::new();

    loop {
        select! {
            recv = conn_closed_rx.recv() => if let Some(id) = recv {
                clients.remove(&id);
                while let Ok(id) = conn_closed_rx.try_recv() {
                    clients.remove(&id);
                }
                debug!("{} TCP connection(s) open.", clients.len());
                waiting_for_free_connections = false;
            } else {
                break;
            },
            con = listener.accept(), if !waiting_for_free_connections => {
                debug!("Trying to accept new client connection.");
                match con {
                    Ok((socket, remote_addr)) => {
                        let id = Uuid::new_v4();
                        debug!("{} TCP connection(s) open.",clients.len());
                        let worterbuch = worterbuch.clone();
                        let conn_closed_tx = conn_closed_tx.clone();

                        let client = subsys.start(SubsystemBuilder::new(format!("client-{id}"), move |s| async move {
                            select! {
                                s = serve(id, remote_addr, worterbuch, socket) => if let Err(e) = s {
                                    error!("Connection to client {id} ({remote_addr:?}) closed with error: {e}");
                                },
                                _ = s.on_shutdown_requested() => (),
                            }
                            conn_closed_tx.send(id).await.ok();
                            Ok::<(),miette::Error>(())
                        }));
                        clients.insert(id, client);
                    },
                    Err(e) => {
                        error!("Error while trying to accept client connection: {e}");
                        warn!("{} TCP connections open, waiting for connections to close.", clients.len());
                        waiting_for_free_connections = true;
                    }
                }
                debug!("Ready to accept new connections.");
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    for (cid, subsys) in clients {
        subsys.initiate_shutdown();
        debug!("Waiting for connection to client {cid} to close …");
        if let Err(e) = subsys.join().await {
            error!("Error waiting for client {cid} to disconnect: {e}");
        }
    }
    debug!("All clients disconnected.");

    drop(listener);

    debug!("tcpserver subsystem completed.");

    Ok(())
}

async fn serve(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
) -> Result<()> {
    info!("New client connected: {client_id} ({remote_addr})");

    if let Err(e) = worterbuch
        .connected(client_id, Some(remote_addr), Protocol::TCP)
        .await
    {
        error!("Error while adding new client: {e}");
    } else {
        debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

        if let Err(e) = serve_loop(client_id, remote_addr, worterbuch.clone(), socket).await {
            error!("Error in serve loop: {e}");
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
) -> Result<()> {
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
                error!("Error sending TCP message: {e}");
                break;
            }
        }
    });

    let tcp_rx = BufReader::new(tcp_rx);
    let mut tcp_rx = tcp_rx.lines();

    let supported_protocol_versions = SUPPORTED_PROTOCOL_VERSIONS.into();

    tcp_send_tx
        .send(ServerMessage::Welcome(Welcome {
            client_id: client_id.to_string(),
            info: ServerInfo::new(
                VERSION.to_owned(),
                supported_protocol_versions,
                authorization_required,
            ),
        }))
        .await
        .into_diagnostic()?;

    let mut proto = Proto::new(
        client_id,
        tcp_send_tx,
        authorization_required,
        config,
        worterbuch,
    );

    loop {
        select! {
            recv = tcp_rx.next_line() => match recv {
                Ok(Some(json)) => {
                    trace!("Processing incoming message …");
                    let msg_processed = proto.process_incoming_message(
                        &json,
                        &mut authorized,
                    ).await?;
                    if !msg_processed {
                        break;
                    }
                    trace!("Processing incoming message done.");
                },
                Ok(None) =>  break,
                Err(e) => {
                    warn!("TCP stream of client {client_id} ({remote_addr}) closed with error:, {e}");
                    break;
                }
            }
        }
    }

    Ok(())
}
