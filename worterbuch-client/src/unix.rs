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

use tokio::{
    io::{BufReader, Lines},
    net::unix::{OwnedReadHalf, OwnedWriteHalf},
    spawn,
    sync::{mpsc, oneshot},
};
use tracing::{debug, error};
use worterbuch_common::{
    ClientMessage, ServerMessage, error::ConnectionResult, write_line_and_flush,
};

const SERVER_ID: &str = "worterbuch server";

pub struct UnixClientSocket {
    tx: mpsc::Sender<ClientMessage>,
    rx: Lines<BufReader<OwnedReadHalf>>,
    closed: oneshot::Receiver<()>,
}

impl UnixClientSocket {
    pub async fn new(
        tx: OwnedWriteHalf,
        rx: Lines<BufReader<OwnedReadHalf>>,
        buffer_size: usize,
    ) -> Self {
        let (send_tx, send_rx) = mpsc::channel(buffer_size);
        let (closed_tx, closed_rx) = oneshot::channel();
        spawn(forward_unix_messages(tx, send_rx, closed_tx));
        Self {
            tx: send_tx,
            rx,
            closed: closed_rx,
        }
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
                debug!("Received message: {json}");
                let sm = serde_json::from_str(&json);
                if let Err(e) = &sm {
                    error!("Error deserializing message '{json}': {e}")
                }
                Ok(sm?)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn close(self) -> ConnectionResult<()> {
        drop(self.tx);
        drop(self.rx);
        self.closed.await.ok();
        Ok(())
    }
}

async fn forward_unix_messages(
    mut tx: OwnedWriteHalf,
    mut send_rx: mpsc::Receiver<ClientMessage>,
    closed_tx: oneshot::Sender<()>,
) {
    while let Some(msg) = send_rx.recv().await {
        if let Err(e) = write_line_and_flush(msg, &mut tx, None, SERVER_ID).await {
            error!("Error sending TCP message: {e}");
            break;
        }
    }

    drop(tx);

    closed_tx.send(()).ok();
}
