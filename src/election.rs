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
use crate::{utils::support_vote, Heartbeat, HeartbeatRequest, PeerMessage, Vote, VoteRequest};
use miette::{Context, IntoDiagnostic, Result};
use tokio::{net::UdpSocket, select, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;

pub enum ElectionOutcome {
    Leader,
    Follower(HeartbeatRequest),
    Cancelled,
}

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

    async fn recv_peer_msg(&self, buf: &mut [u8]) -> Result<Option<PeerMessage>> {
        let received = self
            .socket
            .recv(buf)
            .await
            .into_diagnostic()
            .wrap_err("error receiving peer message")?;
        if received == 0 {
            return Ok(None);
        }

        serde_json::from_slice(&buf[..received])
            .into_diagnostic()
            .wrap_err_with(|| {
                format!(
                    "Could not parse peer message '{}'",
                    String::from_utf8_lossy(&buf[..received])
                )
            })
    }

    async fn run(&mut self) -> Result<ElectionOutcome> {
        let mut buf = [0u8; 65507];

        loop {
            log::info!("Waiting for other candidates or existing leader …");
            let timeout = sleep(self.config.election_timeout());

            select! {
                outcome = self.support_other_candidates() => if let Some(outcome) = outcome? {
                    return Ok(outcome);
                },
                _ = timeout => log::info!("No other candidate asked for votes with high enough priority or sent a heartbeat in time. Trying to become leader myself …"),
            }

            log::info!("Requesting peers to vote for me …");

            self.votes_in_my_favor = 1;
            log::info!(
                "I voted for myself ({}/{}, quorum: {}/{})",
                self.votes_in_my_favor,
                self.config.peer_nodes.len() + 1,
                self.config.quorum,
                self.config.peer_nodes.len() + 1
            );

            // this will allow self-election, which is usually an antipattern. We allow it here to be able to support not fully redundant two-node clusters.
            if self.votes_in_my_favor >= self.config.quorum {
                log::info!("Quorum met, I am now leader.");
                return Ok(ElectionOutcome::Leader);
            }

            self.request_votes().await?;

            let mut timeout = Box::pin(sleep(self.config.heartbeat_timeout()));

            let mut peers = self
                .config
                .peer_nodes
                .iter()
                .map(|p| p.node_id.as_ref())
                .collect::<Vec<&str>>();

            loop {
                select! {
                    _ = &mut timeout => {
                        log::warn!("Could not get enough supporting votes in time, starting over …");
                        break;
                    },
                    msg = self.recv_peer_msg(&mut buf) => if let Some(msg) = msg? {
                        match msg {
                            PeerMessage::Vote(Vote::Response(vote)) => {
                                // making sure we don't count votes from any node twice
                                if !peers.contains(&vote.node_id.as_ref()) {
                                    continue;
                                }
                                peers.retain(|it| it != &vote.node_id);
                                self.votes_in_my_favor += 1;
                                log::info!("Node '{}' voted for me ({}/{}, quorum: {}/{})", vote.node_id, self.votes_in_my_favor, self.config.peer_nodes.len() + 1, self.config.quorum, self.config.peer_nodes.len() + 1);
                                if self.votes_in_my_favor >= self.config.quorum {
                                    log::info!("This instance is now the leader.");
                                    return Ok(ElectionOutcome::Leader);
                                }
                            },
                            PeerMessage::Vote(Vote::Request(vote)) => {
                                log::info!("Looks like node '{}' is also trying to become leader. Traitor!", vote.node_id);
                            },
                            PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                                log::info!("Node '{}' seems to think it's the new leader. Let's see …", heartbeat.peer_info.node_id);
                            },
                            PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                                log::warn!("Node '{}' just sent a heartbeat response. That doesn't make any sense.", heartbeat.node_id);
                            },
                        }
                    },
                    _ = self.subsys.on_shutdown_requested() => return Ok(ElectionOutcome::Cancelled),
                }
            }
        }
    }

    async fn support_other_candidates(&self) -> Result<Option<ElectionOutcome>> {
        let mut buf = [0u8; 65507];
        let prio = self.config.priority().await;
        loop {
            select! {
                msg = self.recv_peer_msg(&mut buf) => if let Some(msg) = msg? {
                    match msg {
                        PeerMessage::Vote(Vote::Response(vote)) => {
                            log::info!("Node '{}' is voting for me, but I haven't requested any votes yet. Weird.", vote.node_id);
                        },
                        PeerMessage::Vote(Vote::Request(vote)) => {
                            if vote.priority >= prio {
                                log::info!("Looks like node '{}' is trying to become leader and has priority {:?} (>= {:?}). Let's support it.", vote.node_id, vote.priority, prio);
                                support_vote(vote.clone(), self.config, self.socket).await?;
                                return self.wait_for_heartbeat(&vote).await;
                            } else {
                                log::info!("Looks like node '{}' is trying to become leader but its priority is too low ({:?} < {:?}). Not supporting it.", vote.node_id, vote.priority, prio);
                            }

                        },
                        PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                            log::info!("Node '{}' seems to be leader. Let's follow it.", heartbeat.peer_info.node_id);
                            return Ok(Some(ElectionOutcome::Follower(heartbeat)));
                        },
                        PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                            log::warn!("Node '{}' just sent a heartbeat response. That doesn't make any sense.", heartbeat.node_id);
                        },
                    }
                },
                _ = self.subsys.on_shutdown_requested() => return Ok(None),
            }
        }
    }

    async fn wait_for_heartbeat(
        &self,
        supported_vote: &VoteRequest,
    ) -> Result<Option<ElectionOutcome>> {
        let mut buf = [0u8; 65507];
        loop {
            select! {
                msg = self.recv_peer_msg(&mut buf) => if let Some(msg) = msg? {
                    match msg {
                        PeerMessage::Vote(Vote::Response(vote)) => {
                            log::info!("Node '{}' is voting for us, be haven't requested any votes yet. Weird.", vote.node_id);
                        },
                        PeerMessage::Vote(Vote::Request(vote)) => {
                            if &vote != supported_vote {
                                log::info!("Looks like node '{}' is also trying to become leader, but we already voted for '{}'. Ignoring it …", vote.node_id, supported_vote.node_id);
                            }
                        },
                        PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                            log::info!("Node '{}' seems to have won the election. Let's follow it.", heartbeat.peer_info.node_id);
                            return Ok(Some(ElectionOutcome::Follower(heartbeat)));
                        },
                        PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                            log::warn!("Node '{}' just sent a heartbeat response. That doesn't make any sense.", heartbeat.node_id);
                        },
                    }
                },
                _ = self.subsys.on_shutdown_requested() => return Ok(None),
            }
        }
    }

    async fn request_votes(&self) -> Result<()> {
        let msg = PeerMessage::Vote(Vote::Request(super::VoteRequest {
            node_id: self.config.node_id.clone(),
            priority: self.config.priority().await,
        }));
        let buf = serde_json::to_vec(&msg).expect("PeerMessage not serializeable");

        for peer in &self.config.peer_nodes {
            log::debug!(
                "Sending vote request to {}@{} …",
                peer.node_id,
                peer.address
            );
            self.socket
                .send_to(&buf, peer.address)
                .await
                .into_diagnostic()
                .wrap_err_with(|| {
                    format!(
                        "could not send peer message to {}@{}",
                        peer.node_id, peer.address
                    )
                })?;
        }

        Ok(())
    }
}

pub async fn elect_leader(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &Config,
) -> Result<ElectionOutcome> {
    let mut election = Election::new(subsys, socket, config);
    select! {
        leader = election.run() => leader,
        _ = subsys.on_shutdown_requested() => Ok(ElectionOutcome::Cancelled),
    }
}
