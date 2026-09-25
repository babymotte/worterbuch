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
    sync::{mpsc, oneshot},
};
use tracing::error;
use worterbuch_common::{
    error::ConnectionResult,
    protocol::v1::{ClientMessage, ServerMessage},
    write_line_and_flush,
};

use crate::CancellationToken;

pub struct TcpClientSocket {
    tx: mpsc::Sender<ClientMessage>,
    rx: Lines<BufReader<OwnedReadHalf>>,
    closed: oneshot::Receiver<()>,
}

impl TcpClientSocket {
    pub(crate) async fn new(
        cancellation_token: CancellationToken,
        tx: OwnedWriteHalf,
        rx: Lines<BufReader<OwnedReadHalf>>,
        send_timeout: Option<Duration>,
        buffer_size: usize,
    ) -> Self {
        let (send_tx, send_rx) = mpsc::channel(buffer_size);
        let (closed_tx, closed_rx) = oneshot::channel();
        spawn(forward_tcp_messages(
            cancellation_token,
            tx,
            send_rx,
            send_timeout,
            closed_tx,
        ));
        Self {
            tx: send_tx,
            rx,
            closed: closed_rx,
        }
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
        worterbuch_common::receive_msg(&mut self.rx, None).await
    }

    pub async fn close(self) -> ConnectionResult<()> {
        drop(self.tx);
        drop(self.rx);
        self.closed.await.ok();
        Ok(())
    }
}

async fn forward_tcp_messages(
    cancellation_token: CancellationToken,
    mut tx: OwnedWriteHalf,
    mut send_rx: mpsc::Receiver<ClientMessage>,
    timeout: Option<Duration>,
    closed_tx: oneshot::Sender<()>,
) {
    while let Some(msg) = send_rx.recv().await {
        if let Err(e) = write_line_and_flush(
            || cancellation_token.clone().cancelled_owned(),
            msg,
            &mut tx,
            timeout,
        )
        .await
        {
            error!("Error sending TCP message: {e}");
            break;
        }
    }

    drop(tx);

    closed_tx.send(()).ok();
}
