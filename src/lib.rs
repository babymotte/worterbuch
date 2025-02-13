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

mod config;
mod election;
mod follower;
mod leader;
mod process_manager;
mod socket;
mod stats;
mod utils;

use config::load_config;
use election::elect_leader;
use follower::follow;
use leader::lead;
use miette::Result;
use serde::{Deserialize, Serialize};
use socket::init_socket;
use stats::start_stats_endpoint;
use std::net::SocketAddr;
use tokio::select;
use tokio_graceful_shutdown::SubsystemHandle;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PeerMessage {
    Vote(Vote),
    Heartbeat(Heartbeat),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Vote {
    Request(VoteRequest),
    Response(VoteResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoteRequest {
    node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoteResponse {
    node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Heartbeat {
    Request(HeartbeatRequest),
    Response(HeartbeatResponse),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    peer_info: PeerInfo,
    public_endpoints: PublicEndpoints,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerInfo {
    node_id: String,
    address: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicEndpoints {
    address: String,
    tcp_port: u16,
    ws_port: u16,
}

pub async fn run(subsys: SubsystemHandle) -> Result<()> {
    let config = load_config().await?;
    let mut socket = init_socket(&config).await?;

    let stats = start_stats_endpoint(&subsys, &config).await?;
    stats.candidate().await;

    let mut leader = false;

    while !subsys.is_shutdown_requested() {
        if leader {
            stats.leader().await;
            select! {
                it = lead(&subsys, &mut socket, &config) => it?,
                _ = subsys.on_shutdown_requested() => break,
            }
        } else {
            select! {
                it = follow(&subsys, &mut socket,  &config, &stats) => it?,
                _ = subsys.on_shutdown_requested() => break,
            }
        }

        stats.candidate().await;
        select! {
            res = elect_leader(&subsys, &mut socket, &config) => leader = res?,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}
