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
use clap::Parser;
use miette::{miette, IntoDiagnostic, Result};
use serde::Deserialize;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    path::Path,
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
        let addrs: Vec<SocketAddr> = value.address.to_socket_addrs().into_diagnostic()?.collect();

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
    #[arg(short, long, default_value = "./config.yaml")]
    config_path: String,
    /// Interval at which leader sends heartbeat to followers (in ms)
    #[arg(short = 'H', long = "heartbeat", default_value = "100")]
    heartbeat_interval: u64,
    /// Minimum time before heartbeat times out (actual time will be longer since a randomized amount of time will be added)
    #[arg(short = 't', long = "timeout", default_value = "500")]
    heartbeat_min_timeout: u64,
    /// Port at which orchestrator will listen for votes and heartbeats from other nodes
    #[arg(short, long, default_value = "8181")]
    port: u16,
    /// The quorum required for a successful leader election vote [default: <number of nodes> / 2 + 1]
    #[arg(short, long)]
    quorum: Option<usize>,
}

fn quorum_sanity_check(quorum: Option<usize>, peers: &[PeerInfo]) -> Result<usize> {
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

    Ok(quorum)
}

async fn load_config_file(path: impl AsRef<Path>) -> Result<ConfigFile> {
    let yaml = fs::read_to_string(path).await.into_diagnostic()?;
    serde_yaml::from_str(&yaml).into_diagnostic()
}

pub struct Config {
    pub node_id: String,
    pub address: SocketAddr,
    pub heartbeat_interval: Duration,
    pub heartbeat_min_timeout: u64,
    pub orchestration_port: u16,
    pub quorum: usize,
    pub peer_nodes: Vec<PeerInfo>,
}

impl Config {
    pub fn heartbeat_timeout(&self) -> Duration {
        let randomized =
            (rand::random::<f64>() * 0.5 * self.heartbeat_min_timeout as f64).round() as u64;
        Duration::from_millis(self.heartbeat_min_timeout + randomized)
    }

    pub fn get_node_addr(&self, node_id: &str) -> Option<SocketAddr> {
        self.peer_nodes
            .iter()
            .find(|p| p.node_id == node_id)
            .map(|p| p.address)
    }
}

pub async fn load_config() -> Result<Config> {
    log::info!("Loading orchestrator config …");

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
                Some(p.to_owned())
            } else {
                None
            }
        })
        .collect();

    log::debug!("Configured nodes: {nodes:?}");
    log::debug!("Configured peers: {peers:?}");

    Ok(Config {
        node_id: args.node_id,
        address,
        heartbeat_interval: Duration::from_millis(args.heartbeat_interval),
        heartbeat_min_timeout: args.heartbeat_min_timeout,
        orchestration_port: args.port,
        quorum: quorum_sanity_check(args.quorum, &nodes)?,
        peer_nodes: peers,
    })
}
