/*
 *  Copyright (C) 2025 Michael Bachmann
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
