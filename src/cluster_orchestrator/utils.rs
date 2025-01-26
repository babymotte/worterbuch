use super::{config::Config, HeartbeatRequest, PeerMessage};
use miette::{IntoDiagnostic, Result};
use tokio::net::UdpSocket;

pub async fn send_heartbeat_requests(config: &Config, socket: &UdpSocket) -> Result<()> {
    // TODO

    Ok(())
}

pub async fn send_heartbeat_response(
    heartbeat: &HeartbeatRequest,
    socket: &UdpSocket,
) -> Result<()> {
    // TODO

    Ok(())
}

pub async fn listen(socket: &UdpSocket, buf: &mut [u8]) -> Result<Option<PeerMessage>> {
    let received = socket.recv(buf).await.into_diagnostic()?;
    if received <= 0 {
        return Ok(None);
    }
    match serde_json::from_slice(&buf[..received]) {
        Ok(msg) => Ok(Some(msg)),
        Err(e) => {
            log::error!("Could not parse peer message: {e}");
            log::debug!("Message: {}", String::from_utf8_lossy(&buf[..received]));
            Ok(None)
        }
    }
}
