mod config;
mod election;
mod follower;
mod leader;
mod socket;
mod utils;

use std::net::SocketAddr;

use config::load_config;
use election::elect_leader;
use follower::follow;
use leader::lead;
use miette::Result;
use serde::{Deserialize, Serialize};
use socket::init_socket;
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
    node_id: String,
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

    let mut leader = false;

    while !subsys.is_shutdown_requested() {
        if leader {
            select! {
                it = lead(&subsys, &mut socket, &config) => it?,
                _ = subsys.on_shutdown_requested() => break,
            }
        } else {
            select! {
                it = follow(&subsys, &mut socket,  &config) => it?,
                _ = subsys.on_shutdown_requested() => break,
            }
        }

        select! {
            res = elect_leader(&subsys, &mut socket, &config) => leader = res?,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}
