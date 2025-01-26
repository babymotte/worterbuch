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

use super::config::Config;
use crate::cluster_orchestrator::{
    utils::{listen, send_heartbeat_requests, support_vote},
    Heartbeat, PeerMessage, Vote,
};
use miette::Result;
use std::{collections::HashMap, time::Instant};
use tokio::{net::UdpSocket, select, time::interval};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};

pub async fn lead(subsys: &SubsystemHandle, socket: &mut UdpSocket, config: &Config) -> Result<()> {
    let wb_server = subsys.start(SubsystemBuilder::new("worterbuch-server", |s| async move {
        log::info!("Starting worterbuch server in leader mode …");

        // TODO start worterbuch server in leader mode
        _ = s.on_shutdown_requested().await;

        log::info!("worterbuch server subsystem stopped.");

        Ok::<(), miette::Error>(())
    }));

    let rest_endpoint = subsys.start(SubsystemBuilder::new("config-endpoint", |s| async move {
        log::info!("Starting config REST endpoint …");

        // TODO open rest endpoint for clients to connect to to request leader socket addresses
        _ = s.on_shutdown_requested().await;

        log::info!("Config REST endpoint subsystem stopped.");

        Ok::<(), miette::Error>(())
    }));

    let mut buf = [0u8; 65507];

    let mut interval = interval(config.heartbeat_interval);

    let mut heartbeat_responses = HashMap::new();

    loop {
        select! {
            _ = interval.tick() => {
                log::trace!("Sending heartbeat …");
                send_heartbeat_requests(config, socket).await?;
                if !check_heartbeat_responses(&heartbeat_responses, config) {
                    break;
                }
            },
            msg = listen(&socket, &mut buf) => {
                match msg? {
                    Some(PeerMessage::Vote(Vote::Request(vote))) => {
                        log::info!("Node '{}' wants to take the lead, let's support it and fall back to follower mode.", vote.node_id);
                        support_vote(vote, config, socket).await?;
                        break;
                    },
                    Some(PeerMessage::Vote(Vote::Response(vote))) => {
                        log::warn!("Node '{}' is sending me vote responses, but I did not start an election. Weird.", vote.node_id);
                    },
                    Some(PeerMessage::Heartbeat(Heartbeat::Request(heartbeat))) => {
                        log::error!("Node '{}' seems to think it is leader. We got ourselves into a split brain scenario. TODO figure out how to resolve this. Dropping to follower status for now.", heartbeat.peer_info.node_id);
                        break;
                    },
                    Some(PeerMessage::Heartbeat(Heartbeat::Response(heartbeat))) => {
                        log::trace!("Received heartbeat response from node '{}'.", heartbeat.node_id);
                        heartbeat_responses.insert(heartbeat.node_id, Instant::now());
                    },
                    None => (),
                }
            }
            res = wb_server.join() => { res?; break; },
            res = rest_endpoint.join() => { res?; break; },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    wb_server.initiate_shutdown();
    rest_endpoint.initiate_shutdown();

    Ok(())
}

fn check_heartbeat_responses(
    heartbeat_responses: &HashMap<String, Instant>,
    config: &Config,
) -> bool {
    let number_of_nodes = config.peer_nodes.len() + 1;
    let mut unresponsive_nodes = 0;
    let mut responsive_nodes = number_of_nodes - unresponsive_nodes;

    for (node_id, timestamp) in heartbeat_responses {
        if timestamp.elapsed().as_millis() > config.heartbeat_min_timeout as u128 {
            unresponsive_nodes += 1;
            responsive_nodes = number_of_nodes - unresponsive_nodes;
            log::warn!(
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
