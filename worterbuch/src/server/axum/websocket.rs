/*
 *  Worterbuch server WebSocket module
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
    auth::JwtClaims,
    server::common::{CloneableWbApi, protocol::Proto},
    stats::VERSION,
};
use axum::extract::ws::{Message, WebSocket};
use futures::{
    sink::SinkExt,
    stream::{SplitSink, StreamExt},
};
use miette::{IntoDiagnostic, Result, bail};
use std::{net::SocketAddr, ops::ControlFlow, time::Duration};
use tokio::{select, sync::mpsc, time::timeout};
use tosub::Subsystem;
use totils::while_select;
use tracing::{debug, error, info, trace};
use worterbuch_common::{
    ClientId, Protocol, WbApi,
    protocol::v1::{ProtocolVersion, ServerInfo, ServerMessage, Welcome},
};

pub(crate) async fn serve(
    subsys: &Subsystem,
    client_id: ClientId,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocket,
    supported_protocol_versions: Box<[ProtocolVersion]>,
) -> Result<()> {
    info!("New client connected: {client_id} ({remote_addr})");

    match worterbuch
        .connected(client_id, Some(remote_addr), Protocol::WS)
        .await
    {
        Ok(ejected) => {
            debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

            if let Err(e) = serve_loop(
                subsys,
                client_id,
                remote_addr,
                worterbuch.named(format!("client/{client_id}")),
                websocket,
                supported_protocol_versions,
                ejected,
            )
            .await
            {
                error!("Error in serve loop: {e}");
            }
        }
        Err(e) => {
            error!("Error while adding new client: {e}");
            eprintln!("{e:?}");
        }
    }

    info!("Client disconnected: {client_id} ({remote_addr})");

    worterbuch
        .disconnected(client_id, Protocol::WS, Some(remote_addr))
        .await?;

    Ok(())
}

type WebSocketSender = SplitSink<WebSocket, Message>;

async fn serve_loop(
    subsys: &Subsystem,
    client_id: ClientId,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocket,
    supported_protocol_versions: Box<[ProtocolVersion]>,
    mut ejected: mpsc::Receiver<()>,
) -> Result<()> {
    let config = worterbuch.config().to_owned();
    let authorization_required = config.auth_token_key.is_some();
    let send_timeout = config.send_timeout;
    let mut authorized = None;

    let (ws_tx, mut ws_rx) = websocket.split();
    let (ws_send_tx, ws_send_rx) = mpsc::channel(config.channel_buffer_size);

    // websocket send loop
    subsys.spawn("send-loop", move |s| {
        send_loop(s, client_id, send_timeout, ws_tx, ws_send_rx)
    });

    ws_send_tx
        .send(ServerMessage::Welcome(Welcome {
            client_id,
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
        ws_send_tx,
        authorization_required,
        config,
        worterbuch,
    );

    while_select! {
        biased;
        _ = ejected.recv() => {
            info!("Client {client_id} was ejected.");
            break;
        },
        recv = ws_rx.next() => process_next_message(recv, client_id, remote_addr, &mut proto, &mut authorized).await?,
    }

    subsys.request_local_shutdown();

    Ok(())
}

async fn process_next_message(
    recv: Option<Result<Message, axum::Error>>,
    client_id: ClientId,
    remote_addr: SocketAddr,
    proto: &mut Proto,
    authorized: &mut Option<JwtClaims>,
) -> Result<ControlFlow<()>> {
    if let Some(msg) = recv {
        match msg {
            Ok(incoming_msg) => {
                debug!("Processing incoming message …");
                if let Message::Text(text) = incoming_msg {
                    let msg_processed = proto.process_incoming_message(&text, authorized).await?;
                    if !msg_processed {
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
            Err(e) => {
                error!("Error in WebSocket connection: {e}");
                return Ok(ControlFlow::Break(()));
            }
        }
    } else {
        debug!("WS stream of client {client_id} ({remote_addr}) closed.");
        return Ok(ControlFlow::Break(()));
    }
    Ok(ControlFlow::Continue(()))
}

async fn send_loop(
    subsys: Subsystem,
    client_id: ClientId,
    send_timeout: Option<Duration>,
    mut ws_tx: SplitSink<WebSocket, Message>,
    mut ws_send_rx: mpsc::Receiver<ServerMessage>,
) -> miette::Result<()> {
    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = ws_send_rx.recv() => if let Some(msg) = recv {
            select! {
                biased;
                _ = subsys.shutdown_requested() => break,
                res = send_with_timeout(&msg, &mut ws_tx, send_timeout, client_id) => {
                    if let Err(e) = res {
                        error!("Error sending WS message '{msg:?}': {e}");
                        break;
                    }
                }
            }
            ControlFlow::Continue(())
        } else {
            break;
        },
    }

    subsys.request_local_shutdown();

    Ok(())
}

async fn send_with_timeout(
    msg: &ServerMessage,
    websocket: &mut WebSocketSender,
    send_timeout: Option<Duration>,
    client_id: ClientId,
) -> Result<()> {
    let json = serde_json::to_string(msg).into_diagnostic()?;
    let msg = Message::Text(json.into());

    if let Some(send_timeout) = send_timeout {
        trace!("Sending with timeout {}s …", send_timeout.as_secs());
        match timeout(send_timeout, websocket.send(msg)).await {
            Ok(r) => r.into_diagnostic()?,
            Err(_) => {
                error!("Send timeout for client {client_id}");
                bail!("Send timeout for client {client_id}");
            }
        }
    } else {
        trace!("Sending without timeout …");
        websocket.send(msg).await.into_diagnostic()?;
    }

    trace!("Sending done.");

    Ok(())
}
