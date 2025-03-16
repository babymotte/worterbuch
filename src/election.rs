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
use crate::{
    Heartbeat, HeartbeatRequest, PeerMessage, Priority, Vote, VoteRequest, VoteResponse,
    config::Peers, utils::support_vote,
};
use miette::{Context, IntoDiagnostic, Result};
use std::{ops::ControlFlow, pin::pin};
use tokio::{net::UdpSocket, select, sync::mpsc, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{Instrument, Level, debug, info, instrument, span, warn};

enum ElectionRoundEvent {
    Timeout,
    PeersChanged(Option<Peers>),
    PeerMessageReceived(Option<PeerMessage>),
    ShutdownRequested,
}

#[derive(Debug)]
pub enum ElectionOutcome {
    Leader,
    Follower(HeartbeatRequest),
    Cancelled,
}

struct Election<'a> {
    subsys: &'a SubsystemHandle,
    socket: &'a mut UdpSocket,
    config: &'a mut Config,
    votes_in_my_favor: usize,
    peers: &'a mut Peers,
    prio: Priority,
}

impl<'a> Election<'a> {
    fn new(
        subsys: &'a SubsystemHandle,
        socket: &'a mut UdpSocket,
        config: &'a mut Config,
        peers: &'a mut Peers,
        prio: Priority,
    ) -> Self {
        Self {
            subsys,
            config,
            socket,
            votes_in_my_favor: 0,
            peers,
            prio,
        }
    }

    // #[instrument(skip(self, buf), level = "trace", err)]
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

    // #[instrument(skip(self, peers_rx), err)]
    async fn run(&mut self, peers_rx: &mut mpsc::Receiver<Peers>) -> Result<ElectionOutcome> {
        let mut buf = [0u8; 65507];

        loop {
            if let ControlFlow::Break(outcome) = self.election_round(peers_rx, &mut buf).await? {
                return Ok(outcome);
            }
        }
    }

    #[instrument(skip(self, peers_rx), err)]
    async fn election_round(
        &mut self,
        peers_rx: &mut mpsc::Receiver<Peers>,
        buf: &mut [u8],
    ) -> Result<ControlFlow<ElectionOutcome>> {
        if let Ok(peers) = peers_rx.try_recv() {
            *self.peers = peers;
            self.config.update_quorum(self.peers)?;
            info!("Config changed, restarting election round.");
            return Ok(ControlFlow::Continue(()));
        }

        info!("Waiting for other candidates or existing leader …");
        let timeout = sleep(self.config.election_timeout());

        select! {
            outcome = self.support_other_candidates() => if let Some(outcome) = outcome? {
                return Ok(ControlFlow::Break(outcome));
            },
            recv = peers_rx.recv() => if let Some(peers) = recv {
                *self.peers = peers;
                self.config.update_quorum(self.peers)?;
                info!("Config changed, restarting election round.");
                return Ok(ControlFlow::Continue(()));
            } else {
                return Ok(ControlFlow::Break(ElectionOutcome::Cancelled));
            },
            _ = timeout => info!("No other candidate asked for votes with high enough priority or sent a heartbeat in time. Trying to become leader myself …"),
        }

        if let Ok(peers) = peers_rx.try_recv() {
            *self.peers = peers;
            self.config.update_quorum(self.peers)?;
            info!("Config changed, restarting election round.");
            return Ok(ControlFlow::Continue(()));
        }

        info!("Requesting peers to vote for me …");

        self.votes_in_my_favor = 1;
        info!(
            "I voted for myself ({}/{}, quorum: {}/{})",
            self.votes_in_my_favor,
            self.peers.peer_nodes().len() + 1,
            self.config.quorum,
            self.peers.peer_nodes().len() + 1,
        );

        // this will allow self-election, which is usually an antipattern. We allow it here to be able to support not fully redundant two-node clusters.
        if self.votes_in_my_favor >= self.config.quorum {
            info!("Quorum met, I am now leader.");
            return Ok(ControlFlow::Break(ElectionOutcome::Leader));
        }

        self.request_votes().await?;

        let sleep_span = span!(Level::INFO, "wait_for_votes");
        let mut timeout = pin!(sleep(self.config.heartbeat_timeout()).instrument(sleep_span));

        let mut peers = self
            .peers
            .peer_nodes()
            .iter()
            .map(|p| p.node_id.to_owned())
            .collect::<Vec<String>>();

        'receive_votes: loop {
            let next_event = select! {
                _ = &mut timeout => ElectionRoundEvent::Timeout,
                recv = peers_rx.recv() => ElectionRoundEvent::PeersChanged(recv),
                msg = self.recv_peer_msg(buf) => ElectionRoundEvent::PeerMessageReceived(msg?),
                _ = self.subsys.on_shutdown_requested() => ElectionRoundEvent::ShutdownRequested
            };

            match next_event {
                ElectionRoundEvent::Timeout => {
                    warn!("Could not get enough supporting votes in time, starting over …");
                    break 'receive_votes;
                }
                ElectionRoundEvent::PeersChanged(peers) => {
                    if let Some(peers) = peers {
                        *self.peers = peers;
                        self.config.update_quorum(self.peers)?;
                        info!("Config changed, restarting election round.");
                        return Ok(ControlFlow::Continue(()));
                    } else {
                        return Ok(ControlFlow::Break(ElectionOutcome::Cancelled));
                    }
                }
                ElectionRoundEvent::PeerMessageReceived(msg) => {
                    if let Some(res) = self.process_peer_election_message(msg, &mut peers).await? {
                        return Ok(res);
                    }
                }
                ElectionRoundEvent::ShutdownRequested => {
                    return Ok(ControlFlow::Break(ElectionOutcome::Cancelled));
                }
            }
        }
        Ok(ControlFlow::Continue(()))
    }

    #[instrument(skip(self), err)]
    async fn process_peer_election_message(
        &mut self,
        msg: Option<PeerMessage>,
        peers: &mut Vec<String>,
    ) -> Result<Option<ControlFlow<ElectionOutcome>>> {
        if let Some(msg) = msg {
            match msg {
                PeerMessage::Vote(Vote::Response(vote)) => {
                    if let Some(res) = self.process_vote_response(vote, peers).await? {
                        return Ok(Some(res));
                    }
                }
                PeerMessage::Vote(Vote::Request(vote)) => {
                    if let Some(res) = self.process_vote_request(vote).await? {
                        return Ok(Some(res));
                    }
                }
                PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                    info!(
                        "Node '{}' seems to think it's the new leader. Let's see …",
                        heartbeat.node_id
                    );
                }
                PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                    warn!(
                        "Node '{}' just sent a heartbeat response. That doesn't make any sense.",
                        heartbeat.node_id
                    );
                }
            }
        }
        Ok(None)
    }

    #[instrument(skip(self), err)]
    async fn process_vote_response(
        &mut self,
        vote: VoteResponse,
        peers: &mut Vec<String>,
    ) -> Result<Option<ControlFlow<ElectionOutcome>>> {
        // making sure we don't count votes from any node twice
        if !peers.contains(&vote.node_id) {
            return Ok(None);
        }
        peers.retain(|it| it != &vote.node_id);
        self.votes_in_my_favor += 1;
        info!(
            "Node '{}' voted for me ({}/{}, quorum: {}/{})",
            vote.node_id,
            self.votes_in_my_favor,
            self.peers.peer_nodes().len() + 1,
            self.config.quorum,
            self.peers.peer_nodes().len() + 1
        );
        if self.votes_in_my_favor >= self.config.quorum {
            info!("This instance is now the leader.");
            return Ok(Some(ControlFlow::Break(ElectionOutcome::Leader)));
        }
        Ok(None)
    }

    #[instrument(skip(self), err)]
    async fn process_vote_request(
        &mut self,
        vote: VoteRequest,
    ) -> Result<Option<ControlFlow<ElectionOutcome>>> {
        if vote.priority >= self.prio {
            info!(
                "Looks like node '{}' is trying to become leader and has priority {:?} (>= {:?}). Let's support it.",
                vote.node_id, vote.priority, self.prio
            );
            self.votes_in_my_favor = self.votes_in_my_favor.saturating_sub(1);
            support_vote(vote.clone(), self.config, self.socket, self.peers).await?;
            if let Some(result) = self.wait_for_heartbeat(&vote).await? {
                return Ok(Some(ControlFlow::Break(result)));
            } else {
                return Ok(Some(ControlFlow::Continue(())));
            }
        } else {
            info!(
                "Looks like node '{}' is trying to become leader, but its priority is too low ({:?} < {:?}). Not supporting it.",
                vote.node_id, vote.priority, self.prio
            );
        }
        Ok(None)
    }

    #[instrument(skip(self), err)]
    async fn support_other_candidates(&self) -> Result<Option<ElectionOutcome>> {
        let mut buf = [0u8; 65507];
        loop {
            select! {
                msg = self.recv_peer_msg(&mut buf) => if let Some(msg) = msg? {
                    match msg {
                        PeerMessage::Vote(Vote::Response(vote)) => {
                            info!("Node '{}' is voting for me, but I haven't requested any votes yet. Weird.", vote.node_id);
                        },
                        PeerMessage::Vote(Vote::Request(vote)) => {
                            if vote.priority >= self.prio {
                                info!("Looks like node '{}' is trying to become leader and has priority {:?} (>= {:?}). Let's support it.", vote.node_id, vote.priority, self.prio);
                                support_vote(vote.clone(), self.config, self.socket, self.peers).await?;
                                return self.wait_for_heartbeat(&vote).await;
                            } else {
                                info!("Looks like node '{}' is trying to become leader, but its priority is too low ({:?} < {:?}). Not supporting it.", vote.node_id, vote.priority, self.prio);
                            }
                        },
                        PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                            info!("Node '{}' seems to be leader. Let's follow it.", heartbeat.node_id);
                            return Ok(Some(ElectionOutcome::Follower(heartbeat)));
                        },
                        PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                            warn!("Node '{}' just sent a heartbeat response. That doesn't make any sense.", heartbeat.node_id);
                        },
                    }
                },
                _ = self.subsys.on_shutdown_requested() => return Ok(None),
            }
        }
    }

    #[instrument(level = Level::TRACE, skip(self), err)]
    async fn wait_for_heartbeat(
        &self,
        supported_vote: &VoteRequest,
    ) -> Result<Option<ElectionOutcome>> {
        let mut timeout = pin!(sleep(self.config.heartbeat_timeout()));
        let mut buf = [0u8; 65507];
        loop {
            select! {
                msg = self.recv_peer_msg(&mut buf) => if let Some(msg) = msg? {
                    match msg {
                        PeerMessage::Vote(Vote::Response(vote)) => {
                            info!("Node '{}' is voting for us, be haven't requested any votes yet. Weird.", vote.node_id);
                        },
                        PeerMessage::Vote(Vote::Request(vote)) => {
                            if &vote != supported_vote {
                                info!("Looks like node '{}' is also trying to become leader, but we already voted for '{}'. Ignoring it …", vote.node_id, supported_vote.node_id);
                            }
                        },
                        PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                            info!("Node '{}' seems to have won the election. Let's follow it.", heartbeat.node_id);
                            return Ok(Some(ElectionOutcome::Follower(heartbeat)));
                        },
                        PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                            warn!("Node '{}' just sent a heartbeat response. That doesn't make any sense.", heartbeat.node_id);
                        },
                    }
                },
                _ = self.subsys.on_shutdown_requested() => return Ok(Some(ElectionOutcome::Cancelled)),
                _ = &mut timeout => {
                    warn!("Didn't get a heartbeat from node '{}' in time, looks like it did not become leader after all.", supported_vote.node_id);
                    return Ok(None);
                }
            }
        }
    }

    #[instrument(skip(self), err)]
    async fn request_votes(&self) -> Result<()> {
        let msg = PeerMessage::Vote(Vote::Request(super::VoteRequest {
            node_id: self.config.node_id.clone(),
            priority: self.prio,
        }));
        let buf = serde_json::to_vec(&msg).expect("PeerMessage not serializeable");

        for peer in self.peers.peer_nodes() {
            debug!(
                "Sending vote request to {}@{} …",
                peer.node_id, peer.address
            );
            self.socket
                .send_to(&buf, peer.raft_addr())
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

// #[instrument(skip(subsys, socket, config, peers_rx))]
pub async fn elect_leader(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &mut Config,
    peers: &mut Peers,
    peers_rx: &mut mpsc::Receiver<Peers>,
    prio: Priority,
) -> Result<ElectionOutcome> {
    let mut election = Election::new(subsys, socket, config, peers, prio);
    select! {
        leader = election.run(peers_rx) => leader,
        _ = subsys.on_shutdown_requested() => Ok(ElectionOutcome::Cancelled),
    }
}
