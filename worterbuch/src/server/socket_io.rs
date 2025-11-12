/*
 *  Worterbuch server socket.io module
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
    server::{CloneableWbApi, common::protocol::Proto},
    stats::VERSION,
};
use socketioxide::{
    SocketIo,
    extract::{Data, SocketRef},
    socket::Sid,
};
use std::{io, time::Duration};
use tokio::{spawn, sync::mpsc, time::timeout};
use tracing::{debug, error, info, trace};
use uuid::Uuid;
use worterbuch_common::{
    ClientMessage, Protocol, ServerInfo, ServerMessage, Value, WbApi, Welcome,
};

const NAME_SPACE: &str = "/";
const EVENT: &str = "wb";

pub(crate) fn init_socket_io(io: SocketIo, worterbuch: CloneableWbApi) {
    info!("Initializing socket.io namespace {NAME_SPACE} …");
    io.ns(NAME_SPACE, |s, d| on_connect(s, d, worterbuch));
}

async fn on_connect(socket: SocketRef, Data(data): Data<Value>, worterbuch: CloneableWbApi) {
    info!(ns = socket.ns(), ?socket.id, ?data, uri = &socket.req_parts().uri.to_string(), "socket.io connected");

    let client_id = Uuid::new_v4();

    spawn(async move {
        if let Err(e) = serve(client_id, socket.id, worterbuch, socket).await {
            error!("Error in serve function: {e}");
        }
    });
}

async fn serve(
    client_id: Uuid,
    socket_client_id: Sid,
    worterbuch: CloneableWbApi,
    socket: SocketRef,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("New client connected: {client_id} ({socket_client_id})");

    if let Err(e) = worterbuch
        .connected(client_id, socket_client_id.to_string(), Protocol::SocketIO)
        .await
    {
        error!("Error while adding new client: {e}");
    } else {
        debug!("Receiving messages from client {client_id} ({socket_client_id}) …",);

        if let Err(e) = serve_loop(client_id, socket_client_id, worterbuch.clone(), socket).await {
            error!("Error in serve loop: {e}");
        }
    }

    worterbuch
        .disconnected(client_id, socket_client_id.to_string())
        .await?;

    Ok(())
}

async fn serve_loop(
    client_id: Uuid,
    socket_client_id: Sid,
    worterbuch: CloneableWbApi,
    socket: SocketRef,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = worterbuch.config().to_owned();
    let authorization_required = config.auth_token_key.is_some();
    let send_timeout = config.send_timeout;
    let mut authorized = None;

    let (socket_tx, mut socket_rx) = mpsc::channel(config.channel_buffer_size);
    let (socket_send_tx, socket_send_rx) = mpsc::channel(config.channel_buffer_size);

    // socket send loop
    spawn(send_loop(
        socket.clone(),
        send_timeout,
        client_id,
        socket_client_id,
        socket_send_rx,
    ));

    let supported_protocol_versions = SUPPORTED_PROTOCOL_VERSIONS.into();

    socket.on(EVENT, async move |Data::<ClientMessage>(data)| {
        trace!(?data, "Received event:");
        socket_tx.send(data).await.ok();
    });

    let wb = worterbuch.clone();
    socket.on_disconnect(async move || {
        wb.disconnected(client_id, socket_client_id.to_string())
            .await
            .ok();
    });

    socket
        // .to(socket_client_id)
        .emit(
            EVENT,
            &Welcome {
                client_id: client_id.to_string(),
                info: ServerInfo::new(
                    VERSION.to_owned(),
                    supported_protocol_versions,
                    authorization_required,
                ),
            },
        )
        // .await
        ?;

    let mut proto = Proto::new(
        client_id,
        socket_send_tx,
        authorization_required,
        config,
        worterbuch,
    );

    loop {
        if let Some(msg) = socket_rx.recv().await {
            trace!("Processing incoming message …");
            let msg_processed = proto.process_incoming_message(msg, &mut authorized).await?;
            if !msg_processed {
                break;
            }
        } else {
            info!("Socket stream of client {client_id} ({socket_client_id}) closed.");
            break;
        }
    }

    Ok(())
}

async fn send_loop(
    socket: SocketRef,
    send_timeout: Option<Duration>,
    client_id: Uuid,
    socket_client_id: Sid,
    mut socket_send_rx: mpsc::Receiver<ServerMessage>,
) {
    while let Some(msg) = socket_send_rx.recv().await {
        if let Err(e) =
            send_with_timeout(msg, &socket, send_timeout, client_id, socket_client_id).await
        {
            error!("Error sending socket.io message: {e}");
            break;
        }
    }
}

async fn send_with_timeout(
    msg: ServerMessage,
    socket: &SocketRef,
    send_timeout: Option<Duration>,
    client_id: Uuid,
    socket_client_id: Sid,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // if let Some(send_timeout) = send_timeout {
    //     trace!(
    //         "Sending {:?} to {} with timeout {:?} …",
    //         msg, socket_client_id, send_timeout
    //     );
    //     match timeout(send_timeout, socket.to(socket_client_id).emit(EVENT, &msg)).await {
    //         Ok(r) => r?,
    //         Err(_) => {
    //             error!("Send timeout for client {client_id}");
    //             return Err(Box::new(io::Error::other(format!(
    //                 "Send timeout for client {client_id}"
    //             ))));
    //         }
    //     }
    // } else {
    trace!(
        "Sending {:?} to {} with timeout {:?} …",
        msg, socket_client_id, send_timeout
    );
    socket.emit(EVENT, &msg)?;
    // }

    trace!("Sending done.");

    Ok(())
}
