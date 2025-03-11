/*
 *  Worterbuch client WebSocket module
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

use futures_util::{SinkExt, StreamExt};
#[cfg(feature = "ws")]
use tokio::net::TcpStream;
#[cfg(feature = "ws")]
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
#[cfg(any(feature = "wasm"))]
use tokio_tungstenite_wasm::{Message, WebSocketStream};
#[cfg(any(feature = "ws", feature = "wasm"))]
use tracing::debug;
use worterbuch_common::{ClientMessage, ServerMessage, error::ConnectionResult};

pub struct WsClientSocket {
    #[cfg(feature = "ws")]
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    #[cfg(feature = "wasm")]
    websocket: WebSocketStream,
}

impl WsClientSocket {
    #[cfg(feature = "ws")]
    pub fn new(websocket: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { websocket }
    }

    #[cfg(any(feature = "wasm"))]
    pub fn new(websocket: WebSocketStream) -> Self {
        Self { websocket }
    }

    pub async fn send_msg(&mut self, msg: &ClientMessage) -> ConnectionResult<()> {
        let json = serde_json::to_string(msg)?;
        debug!("Sending message: {json}");
        let msg = Message::Text(json.into());
        self.websocket.send(msg).await?;
        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        match self.websocket.next().await {
            Some(Ok(Message::Text(json))) => {
                debug!("Received message: {json}");
                let msg = serde_json::from_str(&json)?;
                Ok(Some(msg))
            }
            Some(Err(e)) => Err(e.into()),
            Some(Ok(_)) | None => Ok(None),
        }
    }

    pub async fn close(mut self) -> ConnectionResult<()> {
        #[cfg(any(feature = "ws"))]
        self.websocket
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "client closed".into(),
            }))
            .await?;

        #[cfg(any(feature = "wasm"))]
        self.websocket.close().await?;

        Ok(())
    }
}
