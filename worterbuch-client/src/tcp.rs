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

use std::time::Duration;
use tokio::{
    io::{BufReader, Lines},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    spawn,
    sync::mpsc,
};
use worterbuch_common::{
    error::ConnectionResult,
    tcp::{self, write_line_and_flush},
    ClientMessage, ServerMessage,
};

const SERVER_ID: &str = "worterbuch server";

pub struct TcpClientSocket {
    tx: mpsc::Sender<ClientMessage>,
    rx: Lines<BufReader<OwnedReadHalf>>,
}

impl TcpClientSocket {
    pub async fn new(
        tx: OwnedWriteHalf,
        rx: Lines<BufReader<OwnedReadHalf>>,
        send_timeout: Duration,
        buffer_size: usize,
    ) -> Self {
        let (send_tx, send_rx) = mpsc::channel(buffer_size);
        spawn(forward_tcp_messages(tx, send_rx, send_timeout));
        Self { tx: send_tx, rx }
    }

    pub async fn send_msg(&self, msg: ClientMessage, wait: bool) -> ConnectionResult<()> {
        if wait {
            self.tx.send(msg).await?;
        } else {
            self.tx.try_send(msg)?;
        }

        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        tcp::receive_msg(&mut self.rx).await
    }
}

async fn forward_tcp_messages(
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
