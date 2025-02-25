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
use election::{elect_leader, ElectionOutcome};
use follower::follow;
use leader::lead;
use miette::{IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use socket::init_socket;
use stats::start_stats_endpoint;
use std::{cmp::Ordering, net::SocketAddr, path::Path};
use tokio::{fs::File, select};
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
    priority: Priority,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Priority {
    Primary(i64),
    Secondary(i64),
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Priority::Primary(a), Priority::Primary(b))
            | (Priority::Secondary(a), Priority::Secondary(b)) => b.partial_cmp(a),
            (Priority::Primary(_), Priority::Secondary(_)) => Some(Ordering::Greater),
            (Priority::Secondary(_), Priority::Primary(_)) => Some(Ordering::Less),
        }
    }
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

    while !subsys.is_shutdown_requested() {
        stats.candidate().await;
        let outcome = select! {
            res = elect_leader(&subsys, &mut socket, &config) => res?,
            _ = subsys.on_shutdown_requested() => break,
        };

        match outcome {
            ElectionOutcome::Leader => {
                stats.leader().await;
                select! {
                    it = lead(&subsys, &mut socket, &config) => it?,
                    _ = subsys.on_shutdown_requested() => break,
                }
            }
            ElectionOutcome::Follower(heartbeat) => {
                stats.follower().await;
                select! {
                    it = follow(&subsys, &mut socket,  &config, heartbeat) => it?,
                    _ = subsys.on_shutdown_requested() => break,
                }
            }
            ElectionOutcome::Cancelled => break,
        }
    }

    Ok(())
}

const LEADER_TIMESTAMP_FILE_NAME: &str = ".leader";
const FOLLOWER_TIMESTAMP_FILE_NAME: &str = ".follower";

pub async fn persist_leader_timestamp(path: &Path) -> Result<()> {
    persist_timestamp(&path.join(LEADER_TIMESTAMP_FILE_NAME)).await
}

pub async fn persist_follower_timestamp(path: &Path) -> Result<()> {
    persist_timestamp(&path.join(FOLLOWER_TIMESTAMP_FILE_NAME)).await
}

pub async fn load_millis_since_leader(path: &Path) -> Option<i64> {
    load_millis_since(&path.join(LEADER_TIMESTAMP_FILE_NAME)).await
}
pub async fn load_millis_since_follower(path: &Path) -> Option<i64> {
    load_millis_since(&path.join(FOLLOWER_TIMESTAMP_FILE_NAME)).await
}

async fn persist_timestamp(path: &Path) -> Result<()> {
    File::create(path).await.into_diagnostic()?;
    Ok(())
}

async fn load_millis_since(path: &Path) -> Option<i64> {
    log::debug!("getting metadata of file {path:?}");
    let file = File::open(path).await.ok()?;
    let last_modified = file.metadata().await.ok()?.modified().ok()?;
    log::debug!("{path:?} last modified: {last_modified:?}");
    let elapsed = last_modified.elapsed().ok()?;
    Some(elapsed.as_millis() as i64)
}

#[cfg(test)]
mod lib_test {

    use super::*;

    #[test]
    fn primary_priority_always_takes_precendence() {
        assert!(Priority::Primary(i64::MIN) > Priority::Secondary(i64::MAX));
        assert!(Priority::Primary(i64::MAX) > Priority::Secondary(i64::MIN));
        assert!(Priority::Primary(10) > Priority::Secondary(200));
        assert!(Priority::Primary(200) > Priority::Secondary(10));
        assert!(Priority::Primary(-10) > Priority::Secondary(200));
        assert!(Priority::Primary(200) > Priority::Secondary(-10));
        assert!(Priority::Primary(0) > Priority::Secondary(0));
    }

    #[test]
    fn primary_priorities_are_ordered_by_value() {
        assert!(Priority::Primary(i64::MIN) > Priority::Primary(i64::MAX));
        assert!(Priority::Primary(i64::MAX) < Priority::Primary(i64::MIN));
        assert!(Priority::Primary(10) > Priority::Primary(200));
        assert!(Priority::Primary(200) < Priority::Primary(10));
        assert!(Priority::Primary(-10) > Priority::Primary(200));
        assert!(Priority::Primary(200) < Priority::Primary(-10));
        assert!(Priority::Primary(0) == Priority::Primary(0));
    }

    #[test]
    fn secondary_priorities_are_ordered_by_value() {
        assert!(Priority::Secondary(i64::MIN) > Priority::Secondary(i64::MAX));
        assert!(Priority::Secondary(i64::MAX) < Priority::Secondary(i64::MIN));
        assert!(Priority::Secondary(10) > Priority::Secondary(200));
        assert!(Priority::Secondary(200) < Priority::Secondary(10));
        assert!(Priority::Secondary(-10) > Priority::Secondary(200));
        assert!(Priority::Secondary(200) < Priority::Secondary(-10));
        assert!(Priority::Secondary(0) == Priority::Secondary(0));
    }
}
