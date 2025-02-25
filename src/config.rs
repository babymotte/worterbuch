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

use super::PeerInfo;
use crate::{load_millis_since_follower, load_millis_since_leader, Priority};
use clap::Parser;
use miette::{miette, Context, IntoDiagnostic, Result};
use serde::Deserialize;
use std::{
    collections::HashSet,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::fs;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawPeerInfo {
    node_id: String,
    address: String,
}

impl TryFrom<RawPeerInfo> for PeerInfo {
    type Error = miette::Error;

    fn try_from(value: RawPeerInfo) -> std::result::Result<Self, Self::Error> {
        let addrs: Vec<SocketAddr> = value
            .address
            .to_socket_addrs()
            .into_diagnostic()
            .wrap_err_with(|| {
                format!(
                    "could not resolve address {} of node '{}'",
                    value.address, value.node_id
                )
            })?
            .collect();

        log::debug!(
            "Resolved socket addresses for node {}@{}: {:?}",
            value.node_id,
            value.address,
            addrs
        );

        // TODO use a better selection mechanism than just using the first one

        let address = addrs
            .into_iter()
            .next()
            .ok_or_else(|| miette!("could not resolve address {}", value.address))?;

        Ok(PeerInfo {
            node_id: value.node_id,
            address,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    nodes: Vec<RawPeerInfo>,
}

#[derive(Parser)]
#[command(author, version, about = "Wörterbuch cluster orchestrator", long_about = None)]
struct Args {
    /// The ID of this node
    #[arg()]
    node_id: String,
    /// Path to the cluster config file
    #[arg(
        short,
        long,
        env = "WBCLUSTER_CONGIF_PATH",
        default_value = "./config.yaml"
    )]
    config_path: String,
    /// Interval at which leader sends heartbeat to followers (in ms)
    #[arg(
        short = 'H',
        long = "heartbeat",
        env = "WBCLUSTER_HEARTBEAT_INTERVAL",
        default_value = "100"
    )]
    heartbeat_interval: u64,
    /// Minimum time before heartbeat times out (actual time will be longer since a randomized amount of time will be added)
    #[arg(
        short = 't',
        long = "timeout",
        env = "WBCLUSTER_HEARTBEAT_MIN_TIMEOUT",
        default_value = "500"
    )]
    heartbeat_min_timeout: u64,
    /// Port at which orchestrator will listen for votes and heartbeats from other nodes
    #[arg(short, long, env = "WBCLUSTER_PORT", default_value = "8181")]
    port: u16,
    /// The quorum required for a successful leader election vote [default: <number of nodes> / 2 + 1]
    #[arg(short, long, env = "WBCLUSTER_QUORUM")]
    quorum: Option<usize>,
    /// Port used by followers to sync with the leader
    #[arg(short, long, env = "WBCLUSTER_SYNC_PORT", default_value = "8282")]
    sync_port: u16,
    /// Path to the worterbuch executable. If omitted, it will be looked up from the environment's PATH
    #[arg(
        long,
        short,
        env = "WBCLUSTER_WB_EXECUTABLE",
        default_value = "worterbuch"
    )]
    worterbuch_executable: String,
    /// Port for stats endpoint
    #[arg(long, env = "WBCLUSTER_STATS_PORT", default_value = "8383")]
    stats_port: u16,
    /// Leader election priority
    #[arg(long = "prio", env = "WBCLUSTER_ELECTION_PRIO")]
    priority: Option<i64>,
    /// Data directory
    #[arg(long, env = "WORTERBUCH_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,
}

fn quorum_sanity_check(quorum: Option<usize>, peers: &[PeerInfo]) -> Result<(usize, bool)> {
    let node_count = peers.len();

    let recommended_min_quorum = node_count / 2 + 1;

    let quorum = if let Some(quorum) = quorum {
        quorum
    } else {
        recommended_min_quorum
    };

    if node_count < 1 {
        return Err(miette!(
            "You have not configured any nodes for your cluster."
        ));
    } else if quorum > node_count {
        return Err(miette!("The leader election quorum ({quorum}) is TOO HIGH for the number of nodes ({node_count}). Your cluster will not be able to elect a leader."));
    } else if node_count == 1 {
        log::error!(
            "You have configured only one node, your cluster is NOT redundant! If this is what you want, you should use worterbuch in standalone mode, not as a cluster.",
        );
    } else if quorum == node_count {
        log::error!("The leader election quorum ({quorum}) is TOO HIGH for the number of nodes ({node_count}). If a single node fails your cluster will not be able to elect a new leader, your cluster is NOT redundant!");
    } else if quorum < recommended_min_quorum {
        log::warn!("The leader election quorum ({quorum}) is TOO LOW for the number of nodes ({node_count}). This makes your cluster susceptible to split brain scenarios (i.e. two leaders may be elected at the same time). THIS IS HIGHLY DISCOURAGED!");
    } else if node_count % 2 == 0 {
        log::warn!("An even number of nodes is generally discouraged, since it increases the probability of a tied leader election vote and does not provide any more fail safety than a cluster with one less node.");
    }

    Ok((quorum, quorum < recommended_min_quorum))
}

async fn load_config_file(path: impl AsRef<Path>) -> Result<ConfigFile> {
    let yaml = fs::read_to_string(&path)
        .await
        .into_diagnostic()
        .wrap_err_with(|| {
            format!(
                "could not read config file {}",
                path.as_ref().to_string_lossy()
            )
        })?;
    serde_yaml::from_str(&yaml)
        .into_diagnostic()
        .wrap_err_with(|| format!("could not parse YAML: {}", yaml))
}

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: String,
    pub address: SocketAddr,
    pub heartbeat_interval: Duration,
    pub heartbeat_min_timeout: u64,
    pub orchestration_port: u16,
    pub quorum: usize,
    pub quorum_too_low: bool,
    pub peer_nodes: Vec<PeerInfo>,
    pub sync_port: u16,
    pub worterbuch_executable: String,
    pub stats_port: u16,
    pub data_dir: PathBuf,
    priority: Option<i64>,
}

impl Config {
    pub fn election_timeout(&self) -> Duration {
        let randomized = (rand::random::<f64>() * self.heartbeat_min_timeout as f64).round() as u64;
        Duration::from_millis(self.heartbeat_min_timeout + randomized)
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.heartbeat_min_timeout)
    }

    pub fn get_node_addr(&self, node_id: &str) -> Option<SocketAddr> {
        self.peer_nodes
            .iter()
            .find(|p| p.node_id == node_id)
            .map(|p| p.address)
    }

    pub async fn priority(&self) -> Priority {
        if let Some(prio) = self.priority {
            log::info!("Configured priority: {prio}");
            return Priority::Primary(prio);
        }

        if let Some(prio) = load_millis_since_leader(&self.data_dir).await {
            log::info!("Prio from time since last leader: {prio}");
            return Priority::Primary(prio);
        }

        if let Some(prio) = load_millis_since_follower(&self.data_dir).await {
            log::info!("Prio from time since last follower: {prio}");
            return Priority::Secondary(prio);
        }

        log::info!("No prio configured and never been leader or follower.");
        Priority::Secondary(i64::MAX)
    }
}

pub async fn load_config() -> Result<Config> {
    log::info!("Loading orchestrator config …");

    let mut peer_addresses = HashSet::new();

    let args: Args = Args::parse();
    let config_file = load_config_file(args.config_path).await?;
    let nodes = config_file
        .nodes
        .into_iter()
        .map(PeerInfo::try_from)
        .collect::<Result<Vec<PeerInfo>>>()?;
    let address = nodes
        .iter()
        .find(|p| p.node_id == args.node_id)
        .ok_or_else(|| miette!("No socket address configured for this node."))?
        .address;
    let peers = nodes
        .iter()
        .filter_map(|p| {
            if p.node_id != args.node_id {
                peer_addresses.insert(p.address);
                Some(p.to_owned())
            } else {
                None
            }
        })
        .collect();
    let data_dir = args.data_dir;
    let priority = args.priority;

    log::debug!("Configured nodes: {nodes:?}");
    log::debug!("Configured peers: {peers:?}");

    let (quorum, quorum_too_low) = quorum_sanity_check(args.quorum, &nodes)?;

    Ok(Config {
        node_id: args.node_id,
        address,
        heartbeat_interval: Duration::from_millis(args.heartbeat_interval),
        heartbeat_min_timeout: args.heartbeat_min_timeout,
        orchestration_port: args.port,
        quorum,
        quorum_too_low,
        peer_nodes: peers,
        sync_port: args.sync_port,
        worterbuch_executable: args.worterbuch_executable,
        stats_port: args.stats_port,
        priority,
        data_dir,
    })
}
