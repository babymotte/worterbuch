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

use super::{config::Config, utils::listen};
use crate::cluster_orchestrator::{Heartbeat, PeerMessage, Vote};
use miette::{IntoDiagnostic, Result};
use tokio::{net::UdpSocket, select, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;

struct Election<'a> {
    subsys: &'a SubsystemHandle,
    socket: &'a mut UdpSocket,
    config: &'a Config,
    votes_in_my_favor: usize,
}

impl<'a> Election<'a> {
    fn new(subsys: &'a SubsystemHandle, socket: &'a mut UdpSocket, config: &'a Config) -> Self {
        Self {
            subsys,
            config,
            socket,
            votes_in_my_favor: 0,
        }
    }

    async fn run(&mut self) -> Result<bool> {
        log::info!("Requesting peers to vote for me …");

        self.votes_in_my_favor += 1;

        // this will allow self-election, which is usually an antipattern. We allow it here to be able to support not fully redundant two-node clusters.
        if self.votes_in_my_favor >= self.config.quorum {
            log::info!("I am now the supreme leader!");
            return Ok(true);
        }

        self.request_votes().await?;

        let mut timeout = Box::pin(sleep(self.config.heartbeat_timeout()));

        let mut peers = self
            .config
            .peer_nodes
            .iter()
            .map(|p| p.node_id.as_ref())
            .collect::<Vec<&str>>();

        let mut buf = [0u8; 65507];

        loop {
            select! {
                _ = &mut timeout => {
                    log::warn!("Could not get enough supporting votes in time, returning to follower mode.");
                    return Ok(false);
                },
                msg = self.listen(&mut buf) =>
                    match msg? {
                        Some(PeerMessage::Vote(Vote::Response(vote))) => {
                            // making sure we don't count votes from any node twice
                            if !peers.contains(&vote.node_id.as_ref()) {
                                continue;
                            }
                            peers.retain(|it| it != &vote.node_id);
                            log::info!("Node '{}' voted for me ({}/{}, quorum: {}/{})", vote.node_id, self.votes_in_my_favor, self.config.peer_nodes.len(), self.config.quorum, self.config.peer_nodes.len());
                            self.votes_in_my_favor += 1;
                            if self.votes_in_my_favor >= self.config.quorum {
                                log::info!("I am now the supreme leader!");
                                return Ok(true);
                            }
                        },
                        Some(PeerMessage::Vote(Vote::Request(vote))) => {
                            log::info!("Looks like node '{}' is also trying to become leader. Traitor!", vote.node_id);
                        },
                        Some(PeerMessage::Heartbeat(Heartbeat::Request(heartbeat))) => {
                            log::info!("Node '{}' has apparently been elected, returning to follower mode.", heartbeat.peer_info.node_id);
                            return Ok(false);
                        },
                        Some(PeerMessage::Heartbeat(Heartbeat::Response(heartbeat))) => {
                            log::warn!("Node '{}' just sent a heartbeat response. That doesn't make any sense.", heartbeat.node_id);
                        },
                        None => (),
                    }
                ,
                _ = self.subsys.on_shutdown_requested() => return Ok(false),
            }
        }
    }

    async fn listen(&self, buf: &mut [u8]) -> Result<Option<PeerMessage>> {
        listen(self.socket, buf).await
    }

    async fn request_votes(&self) -> Result<()> {
        let msg = PeerMessage::Vote(Vote::Request(super::VoteRequest {
            node_id: self.config.node_id.clone(),
        }));
        let buf = serde_json::to_vec(&msg).into_diagnostic()?;

        for peer in &self.config.peer_nodes {
            log::debug!(
                "Sending vote request to {}@{} …",
                peer.node_id,
                peer.address
            );
            self.socket
                .send_to(&buf, peer.address)
                .await
                .into_diagnostic()?;
        }

        Ok(())
    }
}

pub async fn elect_leader(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &Config,
) -> Result<bool> {
    loop {
        let mut election = Election::new(subsys, socket, config);

        select! {
            leader = election.run() => return Ok(leader?),
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(false)
}
