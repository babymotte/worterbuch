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
mod error;
mod follower;
mod leader;
mod process_manager;
mod socket;
mod stats;
mod telemetry;
mod utils;

use config::instrument_and_load_config;
use election::{ElectionOutcome, elect_leader};
use follower::follow;
use leader::lead;
use miette::{Context, Result, miette};
use serde::{Deserialize, Serialize};
use socket::init_socket;
use stats::start_stats_endpoint;
use std::{
    cmp::Ordering,
    net::{IpAddr, SocketAddr},
    path::Path,
};
use tokio::{fs::File, sync::mpsc};
use tosub::Subsystem;
use totils::CancelOn;
use tracing::{debug, instrument};

use crate::config::Peers;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Priority(i64);

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
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
    node_id: String,
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
    address: IpAddr,
    raft_port: u16,
    sync_port: u16,
    priority: Option<i64>,
    suicide_on_split_brain: bool,
}

impl PeerInfo {
    pub fn raft_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.raft_port)
    }

    pub fn sync_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.sync_port)
    }
}

pub async fn instrument_and_run_main(subsys: Subsystem) -> Result<()> {
    let (config, peers_rx) = instrument_and_load_config(&subsys)
        .await
        .wrap_err("failed to init telemetry and config")?;
    run_main(subsys, config, peers_rx).await
}

async fn run_main(
    subsys: Subsystem,
    mut config: config::Config,
    mut peers_rx: mpsc::Receiver<(Peers, PeerInfo, Option<usize>)>,
) -> Result<()> {
    let (mut peers, mut me, _) = peers_rx
        .recv()
        .await
        .ok_or_else(|| miette!("peers sender dropped"))?;

    let mut socket = init_socket(&config)
        .await
        .wrap_err("failed to create raft UDP socket")?;

    let stats = start_stats_endpoint(&subsys, config.stats_port)
        .await
        .wrap_err("failed to create stats endpoint")?;

    while !subsys.is_shut_down() {
        let prio = config.priority().await;

        stats.candidate().await;

        let outcome = match elect_leader(
            &subsys,
            &mut socket,
            &mut config,
            &mut peers,
            &mut peers_rx,
            prio,
        )
        .or_cancel_on(subsys.shutdown_requested())
        .await
        {
            Some(it) => it.wrap_err("leader election failed")?,
            None => break,
        };

        match outcome {
            ElectionOutcome::Leader => {
                stats.leader().await;
                match lead(
                    &subsys,
                    &mut socket,
                    &mut config,
                    &mut me,
                    &mut peers,
                    &mut peers_rx,
                )
                .or_cancel_on(subsys.shutdown_requested())
                .await
                {
                    Some(it) => it.wrap_err("error while running as leader")?,
                    None => break,
                }
            }
            ElectionOutcome::Follower(heartbeat) => {
                stats.follower().await;
                match follow(&subsys, &mut socket, &config, &peers, heartbeat)
                    .or_cancel_on(subsys.shutdown_requested())
                    .await
                {
                    Some(it) => it.wrap_err("error while running as follower")?,
                    None => break,
                }
            }
            ElectionOutcome::Cancelled => break,
        }
    }

    Ok(())
}

const TIMESTAMP_FILE_NAME: &str = "last-persisted";

#[instrument(ret)]
pub async fn load_millis_since_active(path: &Path) -> Option<i64> {
    let path = path.join(TIMESTAMP_FILE_NAME);
    debug!("getting metadata of file {path:?}");
    let file = File::open(&path).await.ok()?;
    let last_modified = file.metadata().await.ok()?.modified().ok()?;
    debug!("{path:?} last modified: {last_modified:?}");
    let elapsed = last_modified.elapsed().ok()?;
    Some(elapsed.as_millis() as i64)
}

#[cfg(test)]
mod lib_test {

    use super::*;

    #[test]
    fn priorities_are_ordered_by_reverse_value() {
        assert!(Priority(i64::MIN) > Priority(i64::MAX));
        assert!(Priority(i64::MAX) < Priority(i64::MIN));
        assert!(Priority(10) > Priority(200));
        assert!(Priority(200) < Priority(10));
        assert!(Priority(-10) > Priority(200));
        assert!(Priority(200) < Priority(-10));
        assert!(Priority(0) == Priority(0));
    }
}
