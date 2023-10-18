use tokio::{
    io::{BufReader, Lines},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use worterbuch_common::{
    error::ConnectionResult, tcp::write_line_and_flush, ClientMessage, ServerMessage,
};

pub struct TcpClientSocket {
    tx: OwnedWriteHalf,
    rx: Lines<BufReader<OwnedReadHalf>>,
}

impl TcpClientSocket {
    pub fn new(tx: OwnedWriteHalf, rx: Lines<BufReader<OwnedReadHalf>>) -> Self {
        Self { tx, rx }
    }

    pub async fn send_msg(&mut self, msg: &ClientMessage) -> ConnectionResult<()> {
        write_line_and_flush(msg, &mut self.tx).await?;
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
