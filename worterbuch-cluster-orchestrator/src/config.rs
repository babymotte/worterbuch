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
use crate::{Priority, load_millis_since_active, telemetry};
use clap::Parser;
use miette::{Context, IntoDiagnostic, Result, miette};
use serde::Deserialize;
use std::{
    collections::HashSet,
    net::{SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{fs, select, sync::mpsc, time::interval};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawPeerInfo {
    node_id: String,
    address: String,
    raft_port: u16,
    sync_port: u16,
    priority: Option<i64>,
    suicide_on_split_brain: Option<bool>,
}

impl TryFrom<&RawPeerInfo> for PeerInfo {
    type Error = miette::Error;

    fn try_from(value: &RawPeerInfo) -> std::result::Result<Self, Self::Error> {
        let addrs: Vec<SocketAddr> = format!("{}:{}", value.address, value.raft_port)
            .to_socket_addrs()
            .into_diagnostic()
            .wrap_err_with(|| {
                format!(
                    "could not resolve address {} of node '{}'",
                    value.address, value.node_id
                )
            })?
            .collect();

        debug!(
            "Resolved socket addresses for node {}@{}: {:?}",
            value.node_id, value.address, addrs
        );

        // TODO use a better selection mechanism than just using the first one

        let address = addrs
            .into_iter()
            .next()
            .ok_or_else(|| miette!("could not resolve address {}", value.address))?;

        Ok(PeerInfo {
            node_id: value.node_id.to_owned(),
            address: address.ip(),
            raft_port: value.raft_port,
            sync_port: value.sync_port,
            priority: value.priority,
            suicide_on_split_brain: value.suicide_on_split_brain.unwrap_or(true),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct ConfigFile {
    nodes: Vec<RawPeerInfo>,
    quorum: Option<usize>,
    telemetry: Option<TelemetryConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryConfig {
    pub endpoint: EndpointConfig,
    pub credentials: Option<Credentials>,
    #[serde(default)]
    pub app: AppConfig,
}

impl TelemetryConfig {
    pub fn instance_name(&self, node_id: &str) -> String {
        format!("{}/{}", self.app.name, node_id)
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EndpointConfig {
    Grpc(String),
    Http(String),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials {
    pub user: String,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppConfig {
    pub name: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            name: "worterbuch-cluster-orchestrator".to_owned(),
        }
    }
}

#[derive(Debug, Parser)]
#[command(author, version, about = "Wörterbuch cluster orchestrator", long_about = None)]
struct Args {
    /// The ID of this node
    #[arg()]
    node_id: String,
    /// Path to the cluster config file
    #[arg(
        short,
        long,
        env = "WBCLUSTER_CONFIG_PATH",
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
    /// Data directory
    #[arg(long, env = "WORTERBUCH_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,
    /// Config scan interval in seconds
    #[arg(long, env = "WBCLUSTER_CONFIG_SCAN_INTERVAL", default_value_t = 5)]
    config_scan_interval: u64,
}

fn quorum_sanity_check(quorum: Option<usize>, peers: &[PeerInfo]) -> Result<(usize, bool)> {
    let node_count = peers.len() + 1;

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
        return Err(miette!(
            "The leader election quorum ({quorum}) is TOO HIGH for the number of nodes ({node_count}). Your cluster will not be able to elect a leader."
        ));
    } else if node_count == 1 {
        error!(
            "You have configured only one node, your cluster is NOT redundant! If this is what you want, you should use worterbuch in standalone mode, not as a cluster.",
        );
    } else if quorum == node_count {
        error!(
            "The leader election quorum ({quorum}) is TOO HIGH for the number of nodes ({node_count}). If a single node fails your cluster will not be able to elect a new leader, your cluster is NOT redundant!"
        );
    } else if quorum < recommended_min_quorum {
        warn!(
            "The leader election quorum ({quorum}) is TOO LOW for the number of nodes ({node_count}). This makes your cluster susceptible to split brain scenarios (i.e. two leaders may be elected at the same time). THIS IS HIGHLY DISCOURAGED!"
        );
    } else if node_count.is_multiple_of(2) {
        warn!(
            "An even number of nodes is generally discouraged, since it increases the probability of a tied leader election vote and does not provide any more fail safety than a cluster with one less node."
        );
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
        .wrap_err_with(|| format!("could not parse YAML: {yaml}"))
}

#[derive(Debug, Clone)]
pub struct Peers(Vec<PeerInfo>);

impl Peers {
    pub fn peer_nodes(&self) -> &[PeerInfo] {
        &self.0
    }

    pub fn raft_addr(&self, node_id: &str) -> Option<SocketAddr> {
        self.0
            .iter()
            .find(|p| p.node_id == node_id)
            .map(|p| p.raft_addr())
    }

    pub fn sync_addr(&self, node_id: &str) -> Option<SocketAddr> {
        self.0
            .iter()
            .find(|p| p.node_id == node_id)
            .map(|p| p.sync_addr())
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: String,
    pub heartbeat_interval: Duration,
    pub heartbeat_min_timeout: u64,
    pub raft_port: u16,
    pub quorum: usize,
    pub quorum_too_low: bool,
    pub sync_port: u16,
    pub worterbuch_executable: String,
    pub stats_port: u16,
    pub data_dir: PathBuf,
    pub config_scan_interval: u64,
    pub suicide_on_split_brain: bool,
    priority: Option<i64>,
    pub quorum_configured: Option<usize>,
}

impl Config {
    pub fn election_timeout(&self) -> Duration {
        let randomized = (rand::random::<f64>() * self.heartbeat_min_timeout as f64).round() as u64;
        Duration::from_millis(self.heartbeat_min_timeout + randomized)
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.heartbeat_min_timeout)
    }

    pub async fn priority(&self) -> Priority {
        if let Some(prio) = self.priority {
            debug!("Configured priority: {prio}");
            return Priority(prio);
        }

        if let Some(prio) = load_millis_since_active(&self.data_dir).await {
            debug!("Prio from time since last active: {prio}");
            return Priority(prio);
        }

        debug!("No prio configured and never been leader or follower.");
        Priority(i64::MAX)
    }

    // #[instrument(skip(self))]
    pub fn update_quorum(&mut self, peers: &Peers) -> Result<()> {
        let (quorum, quorum_too_low) =
            quorum_sanity_check(self.quorum_configured, peers.peer_nodes())?;

        self.quorum = quorum;
        self.quorum_too_low = quorum_too_low;

        info!("Updated election quorum to {}.", self.quorum);

        Ok(())
    }

    pub fn update_priority(&mut self, me: &PeerInfo) -> Result<()> {
        self.priority = me.priority;

        info!("Updated node priority to {:?}.", self.priority);

        Ok(())
    }

    pub fn update_suicide_on_split_brain(&mut self, me: &PeerInfo) -> Result<()> {
        self.suicide_on_split_brain = me.suicide_on_split_brain;

        info!(
            "Node will suicide on split brain: {}.",
            self.suicide_on_split_brain
        );

        Ok(())
    }
}

pub async fn instrument_and_load_config(
    subsys: &SubsystemHandle,
) -> Result<(Config, mpsc::Receiver<(Peers, PeerInfo, Option<usize>)>)> {
    let args: Args = Args::parse();
    let config_file = load_config_file(&args.config_path).await?;

    telemetry::init(config_file.telemetry.as_ref(), args.node_id.clone()).await?;

    load_config(subsys, args, config_file).await
}

// #[instrument(skip(subsys), err)]
async fn load_config(
    subsys: &SubsystemHandle,
    args: Args,
    config_file: ConfigFile,
) -> Result<(Config, mpsc::Receiver<(Peers, PeerInfo, Option<usize>)>)> {
    info!("Loading orchestrator config …");
    let (tx, rx) = mpsc::channel(1);
    let mut peer_addresses = HashSet::new();
    let nodes = config_file
        .nodes
        .iter()
        .map(PeerInfo::try_from)
        .collect::<Result<Vec<PeerInfo>>>()?;
    let mut me = None;
    let peers: Vec<PeerInfo> = nodes
        .iter()
        .filter_map(|p| {
            if p.node_id != args.node_id {
                peer_addresses.insert(p.address);
                Some(p.to_owned())
            } else {
                me = Some(p.to_owned());
                None
            }
        })
        .collect();
    let Some(me) = me else {
        return Err(miette!(
            "Node '{}' is not defined in the cluster config.",
            args.node_id
        ));
    };
    debug!("Configured nodes: {nodes:?}");
    debug!("Configured peers: {peers:?}");
    let data_dir = args.data_dir;
    let (quorum, quorum_too_low) = quorum_sanity_check(config_file.quorum, &peers)?;
    let peers = Peers(peers);
    let config = Config {
        node_id: args.node_id.clone(),
        heartbeat_interval: Duration::from_millis(args.heartbeat_interval),
        heartbeat_min_timeout: args.heartbeat_min_timeout,
        raft_port: me.raft_port,
        quorum,
        quorum_too_low,
        sync_port: me.sync_port,
        worterbuch_executable: args.worterbuch_executable,
        stats_port: args.stats_port,
        priority: me.priority,
        suicide_on_split_brain: me.suicide_on_split_brain,
        data_dir,
        quorum_configured: config_file.quorum,
        config_scan_interval: args.config_scan_interval,
    };
    tx.send((peers, me, config_file.quorum)).await.ok();
    let config_path = args.config_path.into();
    let scan_interval = Duration::from_secs(args.config_scan_interval);
    let node_id = args.node_id;
    subsys.start(SubsystemBuilder::new(
        "config-file-watcher",
        async move |s: &mut SubsystemHandle| {
            watch_config_file(s, config_path, tx, scan_interval, config_file, node_id).await
        },
    ));

    Ok((config, rx))
}

async fn watch_config_file(
    subsys: &mut SubsystemHandle,
    path: PathBuf,
    tx: mpsc::Sender<(Peers, PeerInfo, Option<usize>)>,
    scan_interval: Duration,
    config_file: ConfigFile,
    node_id: String,
) -> Result<()> {
    let mut interval = interval(scan_interval);

    let mut config_file = config_file;

    loop {
        select! {
            _ = interval.tick() => config_file = reload_config(subsys, &path, config_file, &node_id, &tx).await?,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}

async fn reload_config(
    subsys: &SubsystemHandle,
    path: &Path,
    config_file: ConfigFile,
    node_id: &str,
    tx: &mpsc::Sender<(Peers, PeerInfo, Option<usize>)>,
) -> Result<ConfigFile> {
    let cf = load_config_file(&path).await?;
    if cf != config_file {
        let nodes = cf
            .nodes
            .iter()
            .map(PeerInfo::try_from)
            .collect::<Result<Vec<PeerInfo>>>()?;

        let mut me = None;
        let peers = nodes
            .iter()
            .filter_map(|p| {
                if p.node_id != node_id {
                    Some(p.to_owned())
                } else {
                    me = Some(p.to_owned());
                    None
                }
            })
            .collect();
        let Some(me) = me else {
            error!("This node is no longer part of the cluster config, shutting down …");
            subsys.request_shutdown();
            return Err(miette!(
                "Node '{}' is not defined in the cluster config.",
                node_id
            ));
        };

        let peers = Peers(peers);

        if tx.try_send((peers, me, cf.quorum)).is_ok() {
            info!("Change in cluster config detected.");
            debug!("Configured nodes changed: {nodes:?}");
        }

        Ok(cf)
    } else {
        Ok(config_file)
    }
}
