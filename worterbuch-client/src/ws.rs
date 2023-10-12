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
        let msg = Message::Text(json);
        self.websocket.send(msg).await?;
        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        match self.websocket.next().await {
            Some(Ok(Message::Text(json))) => {
                let msg = serde_json::from_str(&json)?;
                Ok(Some(msg))
            }
            Some(Err(e)) => Err(e.into()),
            Some(Ok(_)) | None => Ok(None),
        }
    }
}
