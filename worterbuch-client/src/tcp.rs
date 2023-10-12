use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use worterbuch_common::{
    error::ConnectionResult, tcp::write_line_and_flush, ClientMessage, ServerMessage,
};

pub struct TcpClientSocket {
    tx: OwnedWriteHalf,
    rx: BufReader<OwnedReadHalf>,
    buf: String,
}

impl TcpClientSocket {
    pub fn new(tx: OwnedWriteHalf, rx: BufReader<OwnedReadHalf>) -> Self {
        Self {
            tx,
            rx,
            buf: String::new(),
        }
    }

    pub async fn send_msg(&mut self, msg: &ClientMessage) -> ConnectionResult<()> {
        write_line_and_flush(msg, &mut self.tx).await?;
        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        let read = self.rx.read_line(&mut self.buf).await;
        match read {
            Ok(0) => Ok(None),
            Ok(_) => {
                let res = self.buf.clone();
                self.buf.clear();
                let sm = serde_json::from_str(&res)?;
                Ok(sm)
            }
            Err(e) => Err(e.into()),
        }
    }
}
