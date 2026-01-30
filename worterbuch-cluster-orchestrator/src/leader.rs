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
    Heartbeat, PeerInfo, PeerMessage, Vote,
    config::Peers,
    utils::{listen, send_heartbeat_requests},
};
use hashbrown::HashMap;
use miette::Result;
use std::{ops::ControlFlow, time::Instant};
use tokio::{net::UdpSocket, select, sync::mpsc, time::interval};
use tosub::SubsystemHandle;
use tracing::{Level, debug, error, info, instrument, trace, warn};

pub async fn lead(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &mut Config,
    me: &mut PeerInfo,
    peers: &mut Peers,
    peers_rx: &mut mpsc::Receiver<(Peers, PeerInfo, Option<usize>)>,
) -> Result<()> {
    let mut proc_manager = ChildProcessManager::new(subsys, "wb-server-leader", false);

    info!("Starting worterbuch server in leader mode …");

    proc_manager.restart(cmd(config)).await;

    let mut buf = [0u8; 65507];

    let mut heartbeat_interval = interval(config.heartbeat_interval);
    let mut heartbeat_responses = HashMap::new();

    for peer in peers.peer_nodes() {
        heartbeat_responses.insert(peer.node_id.clone(), Instant::now());
    }

    let mut peers_changed: Option<Instant> = None;

    loop {
        let update_quorum = if let Some(instant) = &peers_changed {
            config.quorum_configured.is_some()
                || instant.elapsed().as_secs() > 10 + 2 * config.config_scan_interval
        } else {
            false
        };
        if update_quorum {
            peers_changed = None;
            config.update_quorum(peers)?;
            config.update_priority(me)?;
            config.update_suicide_on_split_brain(me)?;
        }

        select! {
            _ = heartbeat_interval.tick() => {
                trace!("Sending heartbeat …");
                send_heartbeat_requests(config, socket, peers).await?;
                if !check_heartbeat_responses(&heartbeat_responses, config, peers) {
                    break;
                }
            },
            recv = peers_rx.recv() => if let Some((p, m, q)) = recv {
                info!("Number of cluster nodes changed to {}", p.peer_nodes().len() + 1);
                info!("Node priority changed to {:?}", m.priority);
                info!("Quorum changed to {:?}", q);
                info!("Suicide on split brain changed to {:?}", m.suicide_on_split_brain);
                *peers = p;
                config.quorum_configured = q;
                *me = m;
                peers_changed = Some(Instant::now());
            } else {
                break;
            },
            flow = listen(socket, &mut buf, |msg| process_peer_message(
                msg,
                config,
                &mut heartbeat_responses
            )) => if let ControlFlow::Break(_) = flow? {
                break;
            },
            _ = subsys.shutdown_requested() => break,
        }
    }

    proc_manager.stop().await?;

    Ok(())
}

#[instrument(level = Level::TRACE, skip(config))]
async fn process_peer_message(
    msg: PeerMessage,
    config: &Config,
    heartbeat_responses: &mut HashMap<String, Instant>,
) -> Result<ControlFlow<()>> {
    match msg {
        PeerMessage::Vote(Vote::Request(vote)) => {
            info!(
                "Node '{}' wants to take the lead. Over my cold, dead body …",
                vote.node_id
            );
        }
        PeerMessage::Vote(Vote::Response(_)) => {
            // since we assume the leader role as soon as the quorum is met and don't wait until we received a vote from each node there may still be some votes coming in that we have not seen yet, we can ignore those
        }
        PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
            if config.quorum_too_low && config.suicide_on_split_brain {
                error!(
                    "Node '{}' seems to think it is leader. We got ourselves into a split brain scenario. Dropping to follower status to allow re-election.",
                    heartbeat.node_id
                );
                return Ok(ControlFlow::Break(()));
            }
        }
        PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
            trace!(
                "Received heartbeat response from node '{}'.",
                heartbeat.node_id
            );
            heartbeat_responses.insert(heartbeat.node_id, Instant::now());
        }
    }
    Ok(ControlFlow::Continue(()))
}

#[instrument(level = Level::TRACE, skip(config))]
fn check_heartbeat_responses(
    heartbeat_responses: &HashMap<String, Instant>,
    config: &Config,
    peers: &Peers,
) -> bool {
    let mut unresponsive_nodes = peers.peer_nodes().len();
    let mut responsive_nodes = 1;
    let number_of_nodes = unresponsive_nodes + 1;

    for (peer, timestamp) in heartbeat_responses {
        if timestamp.elapsed() <= config.heartbeat_timeout() {
            responsive_nodes += 1;
            unresponsive_nodes = unresponsive_nodes.saturating_sub(1);
        } else {
            debug!(
                "Heartbeat response of node '{}' is overdue ({}/{} node(s) responsive; quorum: {}).",
                peer, responsive_nodes, number_of_nodes, config.quorum
            );
        }
    }

    if responsive_nodes < config.quorum {
        warn!(
            "Quorum of {} is no longer met, too many unresponsive peers. Cluster is no longer able to build a consensus, dropping to follower state to allow re-election.",
            config.quorum
        );
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
            "--instance-name".to_owned(),
            config.node_id.to_owned(),
        ],
    )
}
