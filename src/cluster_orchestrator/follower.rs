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

use super::{config::Config, PeerMessage};
use crate::cluster_orchestrator::{
    utils::{listen, send_heartbeat_response, support_vote},
    Heartbeat, PeerInfo, PublicEndpoints, Vote,
};
use miette::{IntoDiagnostic, Result};
use tokio::{net::UdpSocket, select, sync::mpsc, time::sleep};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};

pub async fn follow(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &Config,
) -> Result<()> {
    let (leader_info_tx_serv, mut leader_info_rx_serv) = mpsc::channel::<PeerInfo>(1);
    let (leader_info_tx_conf, mut leader_info_rx_conf) = mpsc::channel::<PublicEndpoints>(1);

    subsys.start(SubsystemBuilder::new("worterbuch-server", |s| async move {

        loop {
            select! {
                recv = leader_info_rx_serv.recv() => match recv {
                    Some(info) => {
                        log::info!("Received new leader address: {}@{}. (Re-)Starting worterbuch server in follower mode …", info.node_id, info.address);
                        // TODO restart wb server with new leader config
                    },
                    None => break,
                },
                _ = s.on_shutdown_requested() => break,
            }
        }

        log::info!("worterbuch server subsystem stopped.");

        Ok::<(), miette::Error>(())
    }));

    subsys.start(SubsystemBuilder::new("config-endpoint", |s| async move {
        loop {
            select! {
                recv = leader_info_rx_conf.recv() => match recv {
                    Some(endpoints) => {
                        log::info!("Received new leader endpoint info. (Re-)Starting config REST endpoint …");
                        // TODO
                    },
                    None => break,
                },
                _ = s.on_shutdown_requested() => break,
            }
        }

        log::info!("Config REST endpoint subsystem stopped.");

        Ok::<(), miette::Error>(())
    }));

    let mut buf = [0u8; 65507];

    let mut leader_id = None;

    loop {
        select! {
            _ = sleep(config.heartbeat_timeout()) => {
                log::info!("Leader heartbeat timed out. Starting election …");
                break;
            },
            msg = listen(&socket, &mut buf) => {
                match msg? {
                    Some(PeerMessage::Vote(Vote::Request(vote))) => {
                        log::info!("Node '{}' has started a leader election and requested our support. Let's support it.", vote.node_id);
                        support_vote(vote, config, socket).await?;
                    },
                    Some(PeerMessage::Vote(Vote::Response(vote))) => {
                        log::warn!("Node '{}' is sending me vote responses, but I did not start an election. Weird.", vote.node_id);
                    },
                    Some(PeerMessage::Heartbeat(Heartbeat::Request(heartbeat))) => {
                        let this_leader_id = Some(heartbeat.peer_info.node_id.clone());
                        if leader_id != this_leader_id {
                            log::info!("Node '{}' seems to be the new leader.", heartbeat.peer_info.node_id);
                            leader_info_tx_serv.send(heartbeat.peer_info.clone()).await.into_diagnostic()?;
                            leader_info_tx_conf.send(heartbeat.public_endpoints.clone()).await.into_diagnostic()?;
                            leader_id = this_leader_id;
                        }
                        send_heartbeat_response(&heartbeat, config, &socket).await?;
                    },
                    Some(PeerMessage::Heartbeat(Heartbeat::Response(heartbeat))) => {
                        log::warn!("Node '{}' is sending me heartbeat responses but I'm not the leader. Weird.", heartbeat.node_id);
                    },
                    None => (),
                }
            }
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}
