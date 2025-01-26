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
use crate::cluster_orchestrator::{utils::listen, Heartbeat, PeerInfo, PublicEndpoints, Vote};
use miette::{IntoDiagnostic, Result};
use std::future::pending;
use tokio::{net::UdpSocket, select, sync::mpsc, time::sleep};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};

pub async fn follow(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &Config,
) -> Result<()> {
    let (leader_info_tx_serv, mut leader_info_rx_serv) = mpsc::channel::<PeerInfo>(1);
    let (leader_info_tx_conf, mut leader_info_rx_conf) = mpsc::channel::<PublicEndpoints>(1);

    let wb_server = subsys.start(SubsystemBuilder::new("worterbuch-server", |s| async move {
        let mut leader_info: Option<PeerInfo> = None;

        loop {
            select! {
                Some(info) = leader_info_rx_serv.recv() => {
                    log::info!("Received new leader address: {}@{}",info.node_id, info.address);
                    leader_info = Some(info);
                },
                res = async {
                    if let Some(info) = &leader_info {
                        log::info!("Starting worterbuch server in follower mode …");
                        // TODO
                        pending::<()>().await;
                        log::info!("Worterbuch server stopped.");
                        Ok::<(), miette::Error>(())
                    } else {
                        Ok(pending().await)
                    }
                } => res?,
                _ = s.on_shutdown_requested() => break,
            }
        }

        Ok::<(), miette::Error>(())
    }));

    let rest_endpoint = subsys.start(SubsystemBuilder::new("config-endpoint", |s| async move {
        let mut leader_endpoints: Option<PublicEndpoints> = None;

        loop {
            select! {
                Some(endpoints) = leader_info_rx_conf.recv() => {
                    log::info!("Received new leader endpoints.");
                    leader_endpoints = Some(endpoints);
                },
                res = async {
                    if let Some(endpoints) = &leader_endpoints {
                        log::info!("Starting config REST endpoint …");
                        // TODO
                        pending::<()>().await;
                        log::info!("Config endpoint stopped.");
                        Ok::<(), miette::Error>(())
                    } else {
                        Ok(pending().await)
                    }
                } => res?,
                _ = s.on_shutdown_requested() => break,
            }
        }

        Ok::<(), miette::Error>(())
    }));

    // TODO
    // - listen for heartbeats and/or vote requests
    // - on vote request: support new leader
    // - on heartbeat timeout try to become new leader

    let mut buf = [0u8; 65507];

    loop {
        select! {
            _ = sleep(config.heartbeat_timeout()) => {
                log::info!("Leader heartbeat timed out. Starting election …");
                wb_server.initiate_shutdown();
                rest_endpoint.initiate_shutdown();
                break;
            },
            msg = listen(&socket, &mut buf) => {
                match msg? {
                    Some(PeerMessage::Vote(Vote::Request(vote))) => {},
                    Some(PeerMessage::Vote(Vote::Response(vote))) => {},
                    Some(PeerMessage::Heartbeat(Heartbeat::Request(heartbeat))) => {},
                    Some(PeerMessage::Heartbeat(Heartbeat::Response(heartbeat))) => {
                        log::warn!("Node '{}' is sending me heartbeat responses but I'm not the leader. Weird.", heartbeat.node_id);
                    },
                    None => (),
                }
            }
            res = wb_server.join() => { res?; break; },
            res = rest_endpoint.join() => { res?; break; },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
}
