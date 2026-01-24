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
    PeerMessage,
    config::Config,
    process_manager::{ChildProcessManager, CommandDefinition},
};
use crate::{
    Heartbeat, HeartbeatRequest, Vote,
    config::Peers,
    utils::{listen, send_heartbeat_response},
};
use miette::Result;
use std::{net::SocketAddr, ops::ControlFlow, pin::pin};
use tokio::{net::UdpSocket, select, time::sleep};
use tosub::Subsystem;
use tracing::{Level, info, instrument, warn};

pub async fn follow(
    subsys: &Subsystem,
    socket: &mut UdpSocket,
    config: &Config,
    peers: &Peers,
    leader_heartbeat: HeartbeatRequest,
) -> Result<()> {
    let leader_addr = if let Some(it) = peers.sync_addr(&leader_heartbeat.node_id) {
        it
    } else {
        return Ok(());
    };
    let mut proc_manager = ChildProcessManager::new(subsys, "wb-server-follower", true);
    proc_manager.restart(cmd(leader_addr, config)).await;

    let mut buf = [0u8; 65507];

    'outer: loop {
        let mut timeout = pin!(sleep(config.heartbeat_timeout()));

        'inner: loop {
            select! {
                _ = &mut timeout => {
                    info!("Leader heartbeat timed out. Leaving follower mode …");
                    break 'outer;
                },
                flow = listen(socket, &mut buf, |msg| process_peer_message(
                    msg,
                    &leader_heartbeat,
                    config, peers,
                    socket
                )) => if let ControlFlow::Break(_) = flow? {
                    break 'inner;
                },
                _ = subsys.shutdown_requested() => break 'outer,
            }
        }
    }

    proc_manager.stop().await?;

    info!("Worterbuch server follower instance stopped.");

    Ok(())
}

#[instrument(level = Level::TRACE, skip(config, peers, socket))]
async fn process_peer_message(
    msg: PeerMessage,
    leader_heartbeat: &HeartbeatRequest,
    config: &Config,
    peers: &Peers,
    socket: &UdpSocket,
) -> Result<ControlFlow<()>> {
    match msg {
        PeerMessage::Vote(Vote::Request(vote)) => {
            info!(
                "Node '{}' has started a leader election and requested our support, but we are still following '{}'. Ignoring it …",
                vote.node_id, leader_heartbeat.node_id
            );
        }
        PeerMessage::Vote(Vote::Response(vote)) => {
            warn!(
                "Node '{}' is sending us vote responses, but we did not start an election. Weird.",
                vote.node_id
            );
        }
        PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
            if &heartbeat == leader_heartbeat {
                send_heartbeat_response(&heartbeat, config, peers, socket).await?;
                return Ok(ControlFlow::Break(()));
            } else {
                info!(
                    "Node '{}' seems to think its the new leader, but we are still following '{}'. Ignoring it …",
                    heartbeat.node_id, leader_heartbeat.node_id
                );
            }
        }
        PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
            warn!(
                "Node '{}' is sending us heartbeat responses but we are not the leader. Weird.",
                heartbeat.node_id
            );
        }
    }
    Ok(ControlFlow::Continue::<()>(()))
}

fn cmd(leader: SocketAddr, config: &Config) -> CommandDefinition {
    CommandDefinition::new(
        config.worterbuch_executable.to_owned(),
        vec![
            "--follower".to_owned(),
            "--leader-address".to_owned(),
            leader.to_string(),
            "--instance-name".to_owned(),
            config.node_id.to_owned(),
        ],
    )
}
