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
    persist_follower_timestamp,
    utils::{listen, send_heartbeat_response},
    Heartbeat, HeartbeatRequest, Vote,
};
use miette::Result;
use std::{net::IpAddr, ops::ControlFlow, time::Duration};
use tokio::{
    net::UdpSocket,
    select,
    time::{interval, sleep},
};
use tokio_graceful_shutdown::SubsystemHandle;

pub async fn follow(
    subsys: &SubsystemHandle,
    socket: &mut UdpSocket,
    config: &Config,
    leader_heartbeat: HeartbeatRequest,
) -> Result<()> {
    let mut proc_manager = ChildProcessManager::new(subsys, "wb-server-follower", true);
    proc_manager
        .restart(cmd(leader_heartbeat.peer_info.address.ip(), config))
        .await;

    let mut follower_timestamp_interval = interval(Duration::from_secs(1));

    let mut buf = [0u8; 65507];

    loop {
        select! {
            _ = sleep(config.heartbeat_timeout()) => {
                log::info!("Leader heartbeat timed out. Starting election …");
                break;
            },
            _ = follower_timestamp_interval.tick() => {
                persist_follower_timestamp(&config.data_dir).await?;
            },
            flow = listen(socket, &mut buf, |msg| async {
                match msg {
                    PeerMessage::Vote(Vote::Request(vote)) => {
                        log::info!("Node '{}' has started a leader election and requested our support, but we are still following '{}'. Ignoring it …", vote.node_id, leader_heartbeat.peer_info.node_id);
                    },
                    PeerMessage::Vote(Vote::Response(vote)) => {
                        log::warn!("Node '{}' is sending us vote responses, but we did not start an election. Weird.", vote.node_id);
                    },
                    PeerMessage::Heartbeat(Heartbeat::Request(heartbeat)) => {
                        if heartbeat == leader_heartbeat {
                            send_heartbeat_response(&heartbeat, config, socket).await?;
                        } else {
                            log::info!("Node '{}' seems to think its the new leader, but we are still following '{}'. Ignoring it …", heartbeat.peer_info.node_id, leader_heartbeat.peer_info.node_id);
                        }
                    },
                    PeerMessage::Heartbeat(Heartbeat::Response(heartbeat)) => {
                        log::warn!("Node '{}' is sending us heartbeat responses but we are not the leader. Weird.", heartbeat.node_id);
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
