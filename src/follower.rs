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
    PeerMessage,
};
use crate::{
    utils::{listen, send_heartbeat_response, support_vote},
    Heartbeat, Vote,
};
use miette::Result;
use std::{net::IpAddr, ops::ControlFlow};
use tokio::{net::UdpSocket, select, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;

pub async fn follow(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &Config,
) -> Result<()> {
    let mut proc_manager = ChildProcessManager::new(subsys, "wb-server-follower", true);

    let mut buf = [0u8; 65507];

    let mut leader_id = None;

    loop {
        select! {
            _ = sleep(config.heartbeat_timeout()) => {
                log::info!("Leader heartbeat timed out. Starting election …");
                break;
            },
            flow = listen(socket, &mut buf, config, |msg| async {
                match msg {
                    PeerMessage::Vote(Vote::Request(vote)) => {
                        log::info!("Node '{}' has started a leader election and requested our support. Let's support it.", vote.node_id);
                        support_vote(vote, config, socket).await?;
                    },
                    PeerMessage::Vote(Vote::Response(vote)) => {
                        log::warn!("Node '{}' is sending me vote responses, but I did not start an election. Weird.", vote.node_id);
                    },
                    PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                        let this_leader_id = Some(heartbeat.peer_info.node_id.clone());
                        if leader_id != this_leader_id {
                            log::info!("Node '{}' seems to be the new leader. (Re-)Starting worterbuch server in follower mode …", heartbeat.peer_info.node_id);
                            proc_manager.restart(cmd(heartbeat.peer_info.address.ip(), config)).await;
                            leader_id = this_leader_id;
                        }
                        send_heartbeat_response(&heartbeat, config, socket).await?;
                    },
                    PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                        log::warn!("Node '{}' is sending me heartbeat responses but I'm not the leader. Weird.", heartbeat.node_id);
                    }
                }
                Ok(ControlFlow::Continue::<()>(()))
            }) => if let ControlFlow::Break(_) = flow? {
                break;
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    proc_manager.stop().await?;

    log::info!("Worterbuch server follower instance stopped.");

    Ok(())
}

fn cmd(leader: IpAddr, config: &Config) -> CommandDefinition {
    CommandDefinition::new(
        config.worterbuch_executable.to_owned(),
        vec![
            "--follower".to_owned(),
            "--leader-address".to_owned(),
            format!("{}:{}", leader, config.sync_port),
        ],
    )
}
