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

use super::{
    config::Config, HeartbeatRequest, PeerInfo, PeerMessage, PublicEndpoints, VoteRequest,
    VoteResponse,
};
use miette::{IntoDiagnostic, Result};
use std::{future::Future, ops::ControlFlow};
use tokio::net::UdpSocket;

pub async fn send_heartbeat_requests(config: &Config, socket: &UdpSocket) -> Result<()> {
    let peer_info = PeerInfo {
        node_id: config.node_id.clone(),
        address: config.address,
    };

    // TODO get and use actual public endpoints
    let public_endpoints = PublicEndpoints {
        address: config.address.ip().to_string(),
        tcp_port: 8081,
        ws_port: 8080,
    };

    let msg = PeerMessage::Heartbeat(super::Heartbeat::Request(HeartbeatRequest {
        peer_info,
        public_endpoints,
    }));
    let data = serde_json::to_string(&msg).into_diagnostic()?;
    let data = data.as_bytes();

    for peer in &config.peer_nodes {
        socket.send_to(data, peer.address).await.into_diagnostic()?;
    }

    Ok(())
}

pub async fn send_heartbeat_response(
    heartbeat: &HeartbeatRequest,
    config: &Config,
    socket: &UdpSocket,
) -> Result<()> {
    let msg = PeerMessage::Heartbeat(super::Heartbeat::Response(super::HeartbeatResponse {
        node_id: config.node_id.clone(),
    }));
    let data = serde_json::to_string(&msg).into_diagnostic()?;
    let data = data.as_bytes();

    socket
        .send_to(data, heartbeat.peer_info.address)
        .await
        .into_diagnostic()?;

    Ok(())
}

pub async fn support_vote(vote: VoteRequest, config: &Config, socket: &UdpSocket) -> Result<()> {
    if let Some(addr) = config.get_node_addr(&vote.node_id) {
        let resp = PeerMessage::Vote(super::Vote::Response(VoteResponse {
            node_id: config.node_id.to_owned(),
        }));

        let data = serde_json::to_string(&resp).into_diagnostic()?;
        let data = data.as_bytes();

        socket.send_to(data, addr).await.into_diagnostic()?;
    } else {
        log::warn!(
            "Cannot support vote request of noe '{}', no socket address is configured for it!",
            vote.node_id
        );
    }

    Ok(())
}

pub async fn listen<F, T>(
    socket: &UdpSocket,
    buf: &mut [u8],
    config: &Config,
    op: impl FnOnce(PeerMessage) -> F,
) -> Result<ControlFlow<T>>
where
    F: Future<Output = Result<ControlFlow<T>>>,
{
    let (received, addr) = socket.recv_from(buf).await.into_diagnostic()?;

    if received == 0 {
        return Ok(ControlFlow::Continue(()));
    }

    // TODO this fails in kubernetes due to service and pod using different IPs. This needs some stronger form of authentication

    // if !config.is_peer(addr) {
    //     log::warn!("Received peer message from a sender that is not configured as a peer node. Your cluster may be misconfigured or somebody might be trying to DoS it.");
    //     return Ok(ControlFlow::Continue(()));
    // }

    match serde_json::from_slice(&buf[..received]) {
        Ok(msg) => op(msg).await,
        Err(e) => {
            log::error!("Could not parse peer message: {e}");
            log::debug!("Message: {}", String::from_utf8_lossy(&buf[..received]));
            Ok(ControlFlow::Continue(()))
        }
    }
}
