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
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use worterbuch_common::{error::ConnectionResult, ClientMessage, ServerMessage};

pub struct WsClientSocket {
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WsClientSocket {
    pub fn new(websocket: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { websocket }
    }

    pub async fn send_msg(&mut self, msg: &ClientMessage) -> ConnectionResult<()> {
        let json = serde_json::to_string(msg)?;
        log::debug!("Sending message: {json}");
        let msg = Message::Text(json);
        self.websocket.send(msg).await?;
        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        match self.websocket.next().await {
            Some(Ok(Message::Text(json))) => {
                log::debug!("Received messaeg: {json}");
                let msg = serde_json::from_str(&json)?;
                Ok(Some(msg))
            }
            Some(Err(e)) => Err(e.into()),
            Some(Ok(_)) | None => Ok(None),
        }
    }
}
