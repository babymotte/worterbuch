/*
 *  Worterbuch server Unix Socket module
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
use crate::{SUPPORTED_PROTOCOL_VERSIONS, auth::JwtClaims, server::CloneableWbApi, stats::VERSION};
use miette::{IntoDiagnostic, Result};
use std::{collections::HashMap, io, ops::ControlFlow, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    net::{
        UnixListener, UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf, SocketAddr},
    },
    select,
    sync::mpsc,
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;
use worterbuch_common::{
    Protocol, ServerInfo, ServerMessage, WbApi, Welcome, write_line_and_flush,
};

enum SocketEvent {
    Disconnected(Option<Uuid>),
    Connected(Option<Result<(UnixStream, SocketAddr), io::Error>>),
    ShutdownRequested,
}

pub async fn start(
    worterbuch: CloneableWbApi,
    bind_addr: PathBuf,
    subsys: &mut SubsystemHandle,
) -> Result<()> {
    info!(
        "Serving Unix Socket endpoint at {}",
        bind_addr.to_string_lossy()
    );
    tokio::fs::remove_file(&bind_addr).await.ok();
    if let Some(parent) = bind_addr.parent() {
        tokio::fs::create_dir_all(parent).await.into_diagnostic()?;
    }

    let listener = UnixListener::bind(bind_addr.clone()).into_diagnostic()?;

    let (conn_closed_tx, mut conn_closed_rx) = mpsc::channel(100);
    let mut waiting_for_free_connections = false;

    let mut clients = HashMap::new();

    loop {
        let evt = next_socket_event(
            subsys,
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
                    debug!("{} UNIX connection(s) open.", clients.len());
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
                            debug!("{} UNIX connection(s) open.", clients.len());
                            let worterbuch = worterbuch.clone();
                            let conn_closed_tx = conn_closed_tx.clone();

                            let client = subsys.start(SubsystemBuilder::new(format!("client-{id}"), async move |s:&mut SubsystemHandle| {
                            select! {
                                s = serve(s, id, &remote_addr, worterbuch, socket) => if let Err(e) = s {
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
                                "{} UNIX connections open, waiting for connections to close.",
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
    tokio::fs::remove_file(&bind_addr).await.ok();

    debug!("unixsocket subsystem completed.");

    Ok(())
}

async fn next_socket_event(
    subsys: &SubsystemHandle,
    conn_closed_rx: &mut mpsc::Receiver<Uuid>,
    listener: &UnixListener,
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
    remote_addr: &SocketAddr,
    worterbuch: CloneableWbApi,
    socket: UnixStream,
) -> Result<()> {
    info!("New client connected: {client_id} ({remote_addr:?})");

    if let Err(e) = worterbuch.connected(client_id, None, Protocol::UNIX).await {
        error!("Error while adding new client: {e}");
    } else {
        debug!("Receiving messages from client {client_id} ({remote_addr:?}) …",);

        if let Err(e) = serve_loop(subsys, client_id, remote_addr, worterbuch.clone(), socket).await
        {
            error!("Error in serve loop: {e}");
        }
    }

    worterbuch.disconnected(client_id, None).await?;

    Ok(())
}

struct ServeLoop<'a> {
    client_id: Uuid,
    remote_addr: &'a SocketAddr,
    authorized: Option<JwtClaims>,
    unix_rx: Lines<BufReader<OwnedReadHalf>>,
    proto: Proto,
}

async fn serve_loop(
    subsys: &SubsystemHandle,
    client_id: Uuid,
    remote_addr: &SocketAddr,
    worterbuch: CloneableWbApi,
    socket: UnixStream,
) -> Result<()> {
    let config = worterbuch.config().to_owned();
    let authorization_required = config.auth_token_key.is_some();
    let send_timeout = config.send_timeout;
    let authorized = None;

    // unix socket send loop
    let (unix_rx, unix_tx) = socket.into_split();
    let (unix_send_tx, unix_send_rx) = mpsc::channel(config.channel_buffer_size);
    subsys.start(SubsystemBuilder::new(
        "forward_messages_to_socket",
        async move |s: &mut SubsystemHandle| {
            forward_messages_to_socket(s, unix_send_rx, unix_tx, client_id, send_timeout).await
        },
    ));

    let unix_rx = BufReader::new(unix_rx);
    let unix_rx = unix_rx.lines();

    let supported_protocol_versions = SUPPORTED_PROTOCOL_VERSIONS.into();

    unix_send_tx
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
        unix_send_tx,
        authorization_required,
        config,
        worterbuch,
    );

    let serve_loop = ServeLoop {
        authorized,
        client_id,
        proto,
        remote_addr,
        unix_rx,
    };

    serve_loop.run().await
}

async fn forward_messages_to_socket(
    subsys: &mut SubsystemHandle,
    mut unix_send_rx: mpsc::Receiver<ServerMessage>,
    mut unix_tx: OwnedWriteHalf,
    client_id: Uuid,
    send_timeout: Option<Duration>,
) -> Result<()> {
    loop {
        select! {
            recv = unix_send_rx.recv() => if let Some(msg) = recv {
                if let Err(e) = write_line_and_flush(msg, &mut unix_tx, send_timeout, client_id).await {
                    error!("Error sending UNIX message: {e}");
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

impl ServeLoop<'_> {
    async fn run(mut self) -> Result<()> {
        loop {
            let next_line = self.unix_rx.next_line().await;
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
            Err(e) => self.unix_error(e),
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

    fn unix_error(&mut self, e: io::Error) -> std::result::Result<ControlFlow<()>, miette::Error> {
        warn!(
            "UNIX stream of client {} ({:?}) closed with error:, {}",
            self.client_id, self.remote_addr, e
        );
        Ok(ControlFlow::Break(()))
    }

    fn done(&self) -> std::result::Result<ControlFlow<()>, miette::Error> {
        debug!(
            "UNIX stream of client {} ({:?}) closed normally.",
            self.client_id, self.remote_addr
        );
        Ok(ControlFlow::Break(()))
    }
}
