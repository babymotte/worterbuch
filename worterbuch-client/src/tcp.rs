/*
 *  Worterbuch client TCP module
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

use tokio::{
    io::{BufReader, Lines},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    spawn,
    sync::mpsc,
};
use worterbuch_common::{
    error::ConnectionResult, tcp::write_line_and_flush, ClientMessage, ServerMessage,
};

pub struct TcpClientSocket {
    tx: mpsc::UnboundedSender<ClientMessage>,
    rx: Lines<BufReader<OwnedReadHalf>>,
}

impl TcpClientSocket {
    pub async fn new(tx: OwnedWriteHalf, rx: Lines<BufReader<OwnedReadHalf>>) -> Self {
        let (send_tx, send_rx) = mpsc::unbounded_channel();
        spawn(forward_tcp_messages(tx, send_rx));
        Self { tx: send_tx, rx }
    }

    pub async fn send_msg(&self, msg: ClientMessage) -> ConnectionResult<()> {
        self.tx.send(msg)?;
        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        let read = self.rx.next_line().await;
        match read {
            Ok(None) => Ok(None),
            Ok(Some(json)) => {
                log::debug!("Received messaeg: {json}");
                let sm = serde_json::from_str(&json);
                if let Err(e) = &sm {
                    log::error!("Error deserializing message '{json}': {e}")
                }
                Ok(sm?)
            }
            Err(e) => Err(e.into()),
        }
    }
}

async fn forward_tcp_messages(
    mut tx: OwnedWriteHalf,
    mut send_rx: mpsc::UnboundedReceiver<ClientMessage>,
) {
    while let Some(msg) = send_rx.recv().await {
        if let Err(e) = write_line_and_flush(msg, &mut tx).await {
            log::error!("Error sending TCP message: {e}");
            break;
        }
    }
}
