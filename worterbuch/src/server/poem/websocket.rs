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
    server::common::{process_incoming_message, CloneableWbApi},
    stats::VERSION,
};
use futures::{
    sink::SinkExt,
    stream::{SplitSink, StreamExt},
};
use miette::{miette, IntoDiagnostic, Result};
use poem::web::websocket::{Message, WebSocketStream};
use std::{net::SocketAddr, time::Duration};
use tokio::{select, spawn, sync::mpsc, time::sleep};
use uuid::Uuid;
use worterbuch_common::{Protocol, ServerInfo, ServerMessage, Welcome};

pub(crate) async fn serve(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
) -> Result<()> {
    log::info!("New client connected: {client_id} ({remote_addr})");

    if let Err(e) = worterbuch
        .connected(client_id, Some(remote_addr), Protocol::WS)
        .await
    {
        log::error!("Error while adding new client: {e}");
    } else {
        log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

        if let Err(e) = serve_loop(client_id, remote_addr, worterbuch.clone(), websocket).await {
            log::error!("Error in serve loop: {e}");
        }
    }

    worterbuch
        .disconnected(client_id, Some(remote_addr))
        .await?;

    Ok(())
}

type WebSocketSender = SplitSink<WebSocketStream, poem::web::websocket::Message>;

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
) -> Result<()> {
    let config = worterbuch.config().await?;
    let authorization_required = config.auth_token.is_some();
    let send_timeout = config.send_timeout;
    let mut authorized = None;

    let (mut ws_tx, mut ws_rx) = websocket.split();
    let (ws_send_tx, mut ws_send_rx) = mpsc::channel(config.channel_buffer_size);

    // websocket send loop
    spawn(async move {
        while let Some(msg) = ws_send_rx.recv().await {
            if let Err(e) = send_with_timeout(msg, &mut ws_tx, send_timeout, client_id).await {
                log::error!("Error sending WS message: {e}");
                break;
            }
        }
    });

    let protocol_version = worterbuch.supported_protocol_version().await?;

    ws_send_tx
        .send(ServerMessage::Welcome(Welcome {
            client_id: client_id.to_string(),
            info: ServerInfo {
                version: VERSION.to_owned(),
                authorization_required,
                protocol_version,
            },
        }))
        .await
        .into_diagnostic()?;

    loop {
        select! {
            recv = ws_rx.next() => if let Some(msg) = recv {
                match msg {
                    Ok(incoming_msg) => {
                        log::trace!("Processing incoming message …");
                        if let Message::Text(text) = incoming_msg {
                            let (msg_processed, auth) = process_incoming_message(
                                client_id,
                                &text,
                                &worterbuch,
                                &ws_send_tx,
                                authorization_required,
                                authorized,
                                &config
                            )
                            .await?;
                            authorized = auth;
                            if !msg_processed {
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("Error in WebSocket connection: {e}");
                        break;
                    }
                }
            } else {
                log::info!("WS stream of client {client_id} ({remote_addr}) closed.");
                break;
            },
        }
    }

    Ok(())
}

async fn send_with_timeout(
    msg: ServerMessage,
    websocket: &mut WebSocketSender,
    send_timeout: Duration,
    client_id: Uuid,
) -> Result<()> {
    log::trace!("Sending with timeout {}s …", send_timeout.as_secs());
    let json = serde_json::to_string(&msg).into_diagnostic()?;
    let msg = Message::Text(json);
    select! {
        r = websocket.send(msg) => {
            r.into_diagnostic()?;
        },
        _ = sleep(send_timeout) => {
            log::error!("Send timeout for client {client_id}");
            return Err(miette!("Send timeout for client {client_id}"));
        },
    }
    log::trace!("Sending with timeout {}s done.", send_timeout.as_secs());

    Ok(())
}
