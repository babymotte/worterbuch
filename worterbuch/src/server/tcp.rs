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
use crate::{
    SUPPORTED_PROTOCOL_VERSIONS,
    auth::JwtClaims,
    server::common::{CloneableWbApi, init_server_socket},
    stats::VERSION,
};
use miette::{IntoDiagnostic, Result};
use std::{
    collections::HashMap,
    io,
    net::{IpAddr, SocketAddr},
    ops::ControlFlow,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::mpsc,
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use worterbuch_common::{Protocol, ServerInfo, ServerMessage, Welcome, write_line_and_flush};

enum SocketEvent {
    Disconnected(Option<Uuid>),
    Connected(Option<Result<(TcpStream, SocketAddr), io::Error>>),
    ShutdownRequested,
}

pub async fn start(
    worterbuch: CloneableWbApi,
    bind_addr: IpAddr,
    port: u16,
    subsys: SubsystemHandle,
    keepalive_time: Option<Duration>,
    keepalive_interval: Option<Duration>,
    keepalive_retries: Option<u32>,
    send_timeout: Option<Duration>,
) -> Result<()> {
    let addr = format!("{bind_addr}:{port}");

    info!("Serving TCP endpoint at {addr}");

    let listener = init_server_socket(
        bind_addr,
        port,
        keepalive_time,
        keepalive_interval,
        keepalive_retries,
        send_timeout,
    )?;

    let (conn_closed_tx, mut conn_closed_rx) = mpsc::channel(100);
    let mut waiting_for_free_connections = false;

    let mut clients = HashMap::new();
    loop {
        let evt = next_socket_event(
            &subsys,
            &mut conn_closed_rx,
            &listener,
            waiting_for_free_connections,
        )
        .await;

        match evt {
            SocketEvent::Disconnected(uuid) => {
                if let Some(id) = uuid {
                    clients.remove(&id);
                    while let Ok(id) = conn_closed_rx.try_recv() {
                        clients.remove(&id);
                    }
                    debug!("{} TCP connection(s) open.", clients.len());
                    waiting_for_free_connections = false;
                } else {
                    break;
                }
            }
            SocketEvent::Connected(con) => {
                if let Some(con) = con {
                    debug!("Trying to accept new client connection.");
                    match con {
                        Ok((socket, remote_addr)) => {
                            let id = Uuid::new_v4();
                            debug!("{} TCP connection(s) open.", clients.len());
                            let worterbuch = worterbuch.clone();
                            let conn_closed_tx = conn_closed_tx.clone();

                            let client = subsys.start(SubsystemBuilder::new(format!("client-{id}"), move |s| async move {
                            select! {
                                s = serve(&s, id, remote_addr, worterbuch, socket) => if let Err(e) = s {
                                    error!("Connection to client {id} ({remote_addr:?}) closed with error: {e}");
                                },
                                _ = s.on_shutdown_requested() => (),
                            }
                        conn_closed_tx.send(id).await.ok();
                        Ok::<(),miette::Error>(())
                    }));
                            clients.insert(id, client);
                        }
                        Err(e) => {
                            error!("Error while trying to accept client connection: {e}");
                            warn!(
                                "{} TCP connections open, waiting for connections to close.",
                                clients.len()
                            );
                            waiting_for_free_connections = true;
                        }
                    }
                    debug!("Ready to accept new connections.");
                }
            }
            SocketEvent::ShutdownRequested => break,
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

async fn next_socket_event(
    subsys: &SubsystemHandle,
    conn_closed_rx: &mut mpsc::Receiver<Uuid>,
    listener: &TcpListener,
    waiting_for_free_connections: bool,
) -> SocketEvent {
    select! {
        recv = conn_closed_rx.recv() => SocketEvent::Disconnected(recv),
        con = listener.accept() => if !waiting_for_free_connections {
            SocketEvent::Connected(Some(con))
        } else {
            SocketEvent::Connected(None)
        },
        _ = subsys.on_shutdown_requested() => SocketEvent::ShutdownRequested,
    }
}

async fn serve(
    subsys: &SubsystemHandle,
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

        if let Err(e) = serve_loop(subsys, client_id, remote_addr, worterbuch.clone(), socket).await
        {
            error!("Error in serve loop: {e}");
        }
    }

    worterbuch
        .disconnected(client_id, Some(remote_addr))
        .await?;

    Ok(())
}

struct ServeLoop {
    client_id: Uuid,
    remote_addr: SocketAddr,
    authorized: Option<JwtClaims>,
    tcp_rx: Lines<BufReader<OwnedReadHalf>>,
    proto: Proto,
}

async fn serve_loop(
    subsys: &SubsystemHandle,
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    socket: TcpStream,
) -> Result<()> {
    let config = worterbuch.config().await?;
    let authorization_required = config.auth_token.is_some();
    let send_timeout = config.send_timeout;
    let authorized = None;

    // tcp socket send loop
    let (tcp_rx, tcp_tx) = socket.into_split();
    let (tcp_send_tx, tcp_send_rx) = mpsc::channel(config.channel_buffer_size);
    subsys.start(SubsystemBuilder::new(
        "forward_messages_to_socket",
        move |s| forward_messages_to_socket(s, tcp_send_rx, tcp_tx, client_id, send_timeout),
    ));

    let tcp_rx = BufReader::new(tcp_rx);
    let tcp_rx = tcp_rx.lines();

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

    let proto = Proto::new(
        client_id,
        tcp_send_tx,
        authorization_required,
        config,
        worterbuch,
    );

    let serve_loop = ServeLoop {
        authorized,
        client_id,
        proto,
        remote_addr,
        tcp_rx,
    };

    serve_loop.run().await
}

async fn forward_messages_to_socket(
    subsys: SubsystemHandle,
    mut tcp_send_rx: mpsc::Receiver<ServerMessage>,
    mut tcp_tx: OwnedWriteHalf,
    client_id: Uuid,
    send_timeout: Option<Duration>,
) -> Result<()> {
    loop {
        select! {
            recv = tcp_send_rx.recv() => if let Some(msg) = recv {
                if let Err(e) = write_line_and_flush(msg, &mut tcp_tx, send_timeout, client_id).await {
                    error!("Error sending TCP message: {e}");
                    break;
                }
            } else {
                warn!("Message forwarding to client {client_id} stopped: channel closed.");
                break;
            },
            _ = subsys.on_shutdown_requested() => {
                warn!("Message forwarding to client {client_id} stopped: subsystem stopped.");
                break;
            },
        }
    }

    Ok(())
}

impl ServeLoop {
    async fn run(mut self) -> Result<()> {
        loop {
            let next_line = self.tcp_rx.next_line().await;
            if let ControlFlow::Break(it) = self.process_next_line(next_line).await? {
                break Ok(it);
            }
        }
    }

    async fn process_next_line(
        &mut self,
        next_line: Result<Option<String>, io::Error>,
    ) -> Result<ControlFlow<()>> {
        match next_line {
            Ok(Some(json)) => self.process_line(json).await,
            Ok(None) => self.done(),
            Err(e) => self.tcp_error(e),
        }
    }

    async fn process_line(&mut self, json: String) -> Result<ControlFlow<()>> {
        trace!("Processing incoming message …");
        let msg_processed = self
            .proto
            .process_incoming_message(&json, &mut self.authorized)
            .await?;
        if !msg_processed {
            return Ok(ControlFlow::Break(()));
        }
        trace!("Processing incoming message done.");
        Ok(ControlFlow::Continue(()))
    }

    fn tcp_error(&mut self, e: io::Error) -> std::result::Result<ControlFlow<()>, miette::Error> {
        warn!(
            "TCP stream of client {} ({}) closed with error:, {}",
            self.client_id, self.remote_addr, e
        );
        Ok(ControlFlow::Break(()))
    }

    fn done(&self) -> std::result::Result<ControlFlow<()>, miette::Error> {
        debug!(
            "TCP stream of client {} ({}) closed normally.",
            self.client_id, self.remote_addr
        );
        Ok(ControlFlow::Break(()))
    }
}
