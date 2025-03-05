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

use super::{HeartbeatRequest, PeerMessage, VoteRequest, VoteResponse, config::Config};
use crate::config::Peers;
use miette::{Context, IntoDiagnostic, Result};
use std::{future::Future, ops::ControlFlow};
use tokio::net::UdpSocket;
use tracing::{debug, error, warn};

pub async fn send_heartbeat_requests(
    config: &Config,
    socket: &UdpSocket,
    peers: &Peers,
) -> Result<()> {
    let node_id = config.node_id.clone();

    let msg = PeerMessage::Heartbeat(super::Heartbeat::Request(HeartbeatRequest { node_id }));
    let data = serde_json::to_string(&msg).expect("PeerMessage not serializeable");
    let data = data.as_bytes();

    for peer in peers.peer_nodes() {
        socket
            .send_to(data, peer.raft_addr())
            .await
            .into_diagnostic()
            .wrap_err_with(|| {
                format!(
                    "error sending peer message to {}@{}",
                    peer.node_id, peer.address
                )
            })?;
    }

    Ok(())
}

pub async fn send_heartbeat_response(
    heartbeat: &HeartbeatRequest,
    config: &Config,
    peers: &Peers,
    socket: &UdpSocket,
) -> Result<()> {
    let peer_addr = if let Some(it) = peers.raft_addr(&heartbeat.node_id) {
        it
    } else {
        return Ok(());
    };

    let msg = PeerMessage::Heartbeat(super::Heartbeat::Response(super::HeartbeatResponse {
        node_id: config.node_id.clone(),
    }));
    let data = serde_json::to_string(&msg).expect("PeerMessage not serializeable");
    let data = data.as_bytes();

    socket
        .send_to(data, peer_addr)
        .await
        .into_diagnostic()
        .wrap_err_with(|| {
            format!(
                "error sending peer message to {}@{}",
                heartbeat.node_id, peer_addr
            )
        })?;

    Ok(())
}

pub async fn support_vote(
    vote: VoteRequest,
    config: &Config,
    socket: &UdpSocket,
    peers: &Peers,
) -> Result<()> {
    if let Some(addr) = peers.raft_addr(&vote.node_id) {
        let resp = PeerMessage::Vote(super::Vote::Response(VoteResponse {
            node_id: config.node_id.to_owned(),
        }));

        let data = serde_json::to_string(&resp).expect("PeerMessage not serializeable");
        let data = data.as_bytes();

        socket
            .send_to(data, addr)
            .await
            .into_diagnostic()
            .wrap_err_with(|| format!("error sending peer message to {}@{}", vote.node_id, addr))?;
    } else {
        warn!(
            "Cannot support vote request of noe '{}', no socket address is configured for it!",
            vote.node_id
        );
    }

    Ok(())
}

pub async fn listen<F, T>(
    socket: &UdpSocket,
    buf: &mut [u8],
    op: impl FnOnce(PeerMessage) -> F,
) -> Result<ControlFlow<T>>
where
    F: Future<Output = Result<ControlFlow<T>>>,
{
    let received = socket
        .recv(buf)
        .await
        .into_diagnostic()
        .wrap_err("error receiving peer message")?;

    if received == 0 {
        return Ok(ControlFlow::Continue(()));
    }

    match serde_json::from_slice(&buf[..received]) {
        Ok(msg) => op(msg).await,
        Err(e) => {
            error!("Could not parse peer message: {e}");
            debug!("Message: {}", String::from_utf8_lossy(&buf[..received]));
            Ok(ControlFlow::Continue(()))
        }
    }
}
