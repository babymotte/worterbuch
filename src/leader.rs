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
    config::Config,
    process_manager::{ChildProcessManager, CommandDefinition},
};
use crate::{
    persist_leader_timestamp,
    utils::{listen, send_heartbeat_requests},
    Heartbeat, PeerMessage, Vote,
};
use miette::Result;
use std::{
    collections::HashMap,
    ops::ControlFlow,
    time::{Duration, Instant},
};
use tokio::{net::UdpSocket, select, time::interval};
use tokio_graceful_shutdown::SubsystemHandle;

pub async fn lead(subsys: &SubsystemHandle, socket: &mut UdpSocket, config: &Config) -> Result<()> {
    let mut proc_manager = ChildProcessManager::new(subsys, "wb-server-leader", false);

    log::info!("Starting worterbuch server in leader mode …");

    proc_manager.restart(cmd(config)).await;

    let mut buf = [0u8; 65507];

    let mut heartbeat_interval = interval(config.heartbeat_interval);
    let mut leader_timestamp_interval = interval(Duration::from_secs(1));

    let mut heartbeat_responses = HashMap::new();

    for peer in &config.peer_nodes {
        heartbeat_responses.insert(peer.node_id.clone(), Instant::now());
    }

    loop {
        select! {
            _ = heartbeat_interval.tick() => {
                log::trace!("Sending heartbeat …");
                send_heartbeat_requests(config, socket).await?;
                if !check_heartbeat_responses(&heartbeat_responses, config) {
                    break;
                }
            },
            _ = leader_timestamp_interval.tick() => {
                persist_leader_timestamp(&config.data_dir).await?;
            },
            flow = listen(socket, &mut buf, |msg| async {
                match msg {
                    PeerMessage::Vote(Vote::Request(vote)) => {
                        log::info!("Node '{}' wants to take the lead. Over my cold, dead body …", vote.node_id);
                    },
                    PeerMessage::Vote(Vote::Response(_)) => {
                        // since we assume the leader role as soon as the quorum is met and don't wait until we received a vote from each node there may still be some votes coming in that we have not seen yet, we can ignore those
                    },
                    PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                        if config.quorum_too_low {
                            log::error!("Node '{}' seems to think it is leader. We got ourselves into a split brain scenario. Dropping to follower status to allow re-election.", heartbeat.peer_info.node_id);
                            return Ok(ControlFlow::Break(()));
                        }
                    },
                    PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                        log::trace!("Received heartbeat response from node '{}'.", heartbeat.node_id);
                        heartbeat_responses.insert(heartbeat.node_id, Instant::now());
                    },
                }
                Ok(ControlFlow::Continue(()))
            }) => if let ControlFlow::Break(_) = flow? {
                break;
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    persist_leader_timestamp(&config.data_dir).await?;

    proc_manager.stop().await?;

    Ok(())
}

fn check_heartbeat_responses(
    heartbeat_responses: &HashMap<String, Instant>,
    config: &Config,
) -> bool {
    let number_of_nodes = config.peer_nodes.len() + 1;
    let mut unresponsive_nodes = 0;
    let mut responsive_nodes;

    for (node_id, timestamp) in heartbeat_responses {
        if timestamp.elapsed() > config.heartbeat_timeout() {
            unresponsive_nodes += 1;
            responsive_nodes = number_of_nodes - unresponsive_nodes;
            log::debug!(
                "Heartbeat response of node '{}' is overdue ({}/{} node(s) responsive; quorum: {}).",
                node_id,
                responsive_nodes, number_of_nodes, config.quorum
            );
        }
    }

    if number_of_nodes - unresponsive_nodes < config.quorum {
        log::warn!("Quorum of {} is no longer met, too many unresponsive peers. Cluster is no longer able to build a consensus, dropping to follower state to allow re-election.", config.quorum);
        false
    } else {
        true
    }
}

fn cmd(config: &Config) -> CommandDefinition {
    CommandDefinition::new(
        config.worterbuch_executable.to_owned(),
        vec![
            "--leader".to_owned(),
            "--sync-port".to_owned(),
            config.sync_port.to_string(),
        ],
    )
}
