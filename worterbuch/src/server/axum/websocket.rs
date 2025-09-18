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
    SUPPORTED_PROTOCOL_VERSIONS,
    server::common::{CloneableWbApi, protocol::Proto},
    stats::VERSION,
};
use axum::extract::ws::{Message, WebSocket};
use futures::{
    sink::SinkExt,
    stream::{SplitSink, StreamExt},
};
use miette::{IntoDiagnostic, Result, miette};
use std::{net::SocketAddr, time::Duration};
use tokio::{spawn, sync::mpsc, time::timeout};
use tracing::{debug, error, info, trace};
use uuid::Uuid;
use worterbuch_common::{Protocol, ServerInfo, ServerMessage, Welcome};

pub(crate) async fn serve(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocket,
) -> Result<()> {
    info!("New client connected: {client_id} ({remote_addr})");

    if let Err(e) = worterbuch
        .connected(client_id, Some(remote_addr), Protocol::WS)
        .await
    {
        error!("Error while adding new client: {e}");
    } else {
        debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

        if let Err(e) = serve_loop(client_id, remote_addr, worterbuch.clone(), websocket).await {
            error!("Error in serve loop: {e}");
        }
    }

    worterbuch
        .disconnected(client_id, Some(remote_addr))
        .await?;

    Ok(())
}

type WebSocketSender = SplitSink<WebSocket, Message>;

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocket,
) -> Result<()> {
    let config = worterbuch.config().await?;
    let authorization_required = config.auth_token_key.is_some();
    let send_timeout = config.send_timeout;
    let mut authorized = None;

    let (ws_tx, mut ws_rx) = websocket.split();
    let (ws_send_tx, ws_send_rx) = mpsc::channel(config.channel_buffer_size);

    // websocket send loop
    spawn(send_loop(client_id, send_timeout, ws_tx, ws_send_rx));

    let supported_protocol_versions = SUPPORTED_PROTOCOL_VERSIONS.into();

    ws_send_tx
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
        ws_send_tx,
        authorization_required,
        config,
        worterbuch,
    );

    loop {
        if let Some(msg) = ws_rx.next().await {
            match msg {
                Ok(incoming_msg) => {
                    trace!("Processing incoming message …");
                    if let Message::Text(text) = incoming_msg {
                        let msg_processed = proto
                            .process_incoming_message(&text, &mut authorized)
                            .await?;
                        if !msg_processed {
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("Error in WebSocket connection: {e}");
                    break;
                }
            }
        } else {
            info!("WS stream of client {client_id} ({remote_addr}) closed.");
            break;
        }
    }

    Ok(())
}

async fn send_loop(
    client_id: Uuid,
    send_timeout: Option<Duration>,
    mut ws_tx: SplitSink<WebSocket, Message>,
    mut ws_send_rx: mpsc::Receiver<ServerMessage>,
) {
    while let Some(msg) = ws_send_rx.recv().await {
        if let Err(e) = send_with_timeout(msg, &mut ws_tx, send_timeout, client_id).await {
            error!("Error sending WS message: {e}");
            break;
        }
    }
}

async fn send_with_timeout(
    msg: ServerMessage,
    websocket: &mut WebSocketSender,
    send_timeout: Option<Duration>,
    client_id: Uuid,
) -> Result<()> {
    let json = serde_json::to_string(&msg).into_diagnostic()?;
    let msg = Message::Text(json.into());

    if let Some(send_timeout) = send_timeout {
        trace!("Sending with timeout {}s …", send_timeout.as_secs());
        match timeout(send_timeout, websocket.send(msg)).await {
            Ok(r) => r.into_diagnostic()?,
            Err(_) => {
                error!("Send timeout for client {client_id}");
                return Err(miette!("Send timeout for client {client_id}"));
            }
        }
    } else {
        trace!("Sending without timeout …");
        websocket.send(msg).await.into_diagnostic()?;
    }

    trace!("Sending done.");

    Ok(())
}
