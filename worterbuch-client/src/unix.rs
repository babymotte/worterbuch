/*
 *  Worterbuch client Unix Socket module
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

use std::time::Duration;

use tokio::{
    io::{BufReader, Lines},
    net::unix::{OwnedReadHalf, OwnedWriteHalf},
    spawn,
    sync::mpsc,
};
use worterbuch_common::{
    error::ConnectionResult, tcp::write_line_and_flush, ClientMessage, ServerMessage,
};

const SERVER_ID: &str = "worterbuch server";

pub struct UnixClientSocket {
    tx: mpsc::Sender<ClientMessage>,
    rx: Lines<BufReader<OwnedReadHalf>>,
}

impl UnixClientSocket {
    pub async fn new(
        tx: OwnedWriteHalf,
        rx: Lines<BufReader<OwnedReadHalf>>,
        send_timeout: Duration,
        buffer_size: usize,
    ) -> Self {
        let (send_tx, send_rx) = mpsc::channel(buffer_size);
        spawn(forward_unix_messages(tx, send_rx, send_timeout));
        Self { tx: send_tx, rx }
    }

    pub async fn send_msg(&self, msg: ClientMessage) -> ConnectionResult<()> {
        self.tx.send(msg).await?;
        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        let read = self.rx.next_line().await;
        match read {
            Ok(None) => Ok(None),
            Ok(Some(json)) => {
                log::debug!("Received message: {json}");
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

async fn forward_unix_messages(
    mut tx: OwnedWriteHalf,
    mut send_rx: mpsc::Receiver<ClientMessage>,
    timeout: Duration,
) {
    while let Some(msg) = send_rx.recv().await {
        if let Err(e) = write_line_and_flush(msg, &mut tx, timeout, SERVER_ID).await {
            log::error!("Error sending TCP message: {e}");
            break;
        }
    }
}
