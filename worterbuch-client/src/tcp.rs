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
