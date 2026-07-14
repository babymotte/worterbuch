/*
 *  Types and helper functions for proxy mode
 *
 *  Copyright (C) 2024 Michael Bachmann
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

use crate::{
    Config, Servers,
    cluster::{
        Mode,
        follower::try_process_leader_message,
        protocol::{LeaderSyncMessage, StateSync},
        shutdown,
    },
    error::{WorterbuchAppError, WorterbuchAppResult},
    persistence::unlock_persistence,
    server::common::WbFunction,
    worterbuch::Worterbuch,
};
use serde_json::json;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    ops::ControlFlow,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    select,
    sync::mpsc,
};
use tosub::SubsystemHandle;
use tracing::{debug, info, warn};
use worterbuch_common::{
    INTERNAL_CLIENT_ID,
    error::ConfigError,
    protocol::{InternalAction, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, Trace},
    receive_msg, topic, while_select,
};

pub(crate) async fn run(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    servers: Servers,
    leader_addresses: Vec<String>,
) -> WorterbuchAppResult<()> {
    #[cfg(feature = "commercial")]
    if !config.license.features.proxy {
        return Err(crate::error::WorterbuchAppError::NoLicense(
            "proxy".to_owned(),
        ));
    }

    let leader_addresses = leader_addresses
        .iter()
        .map(ToSocketAddrs::to_socket_addrs)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            WorterbuchAppError::ConfigError(ConfigError::InvalidLeaderAddress(e, leader_addresses))
        })?
        .into_iter()
        .flatten()
        .collect::<Vec<SocketAddr>>();

    info!("Running in PROXY mode. Leaders: {:?}", leader_addresses);

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Proxy),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::Startup),
            true,
        )
        .await?;

    // TODO get from config
    let retry_seconds = 1;

    let mut counter = 0;

    'outer: loop {
        for leader_address in &leader_addresses {
            if counter >= leader_addresses.len() {
                warn!("Could not connect to any leader. Retrying in {retry_seconds} second(s) …");
                select! {
                    biased;
                    _ = subsys.shutdown_requested() => break 'outer,
                    _ = tokio::time::sleep(Duration::from_secs(retry_seconds)) => counter = 0,
                }
            }

            select! {
                biased;
                _ = subsys.shutdown_requested() => break 'outer,
                    res = run_with_leader(
                    subsys,
                    &mut worterbuch,
                    &mut api_rx,
                    config.clone(),
                    *leader_address,
                ) => {
                    let initial_connection_successful =  res?;
                    if initial_connection_successful {
                        info!("Connection to leader {} lost. Trying next leader …", leader_address);
                        counter = 1;
                    } else {
                        info!("Could not connect to leader {}. Trying next leader …", leader_address);
                        counter += 1;
                    }
                },
            }
        }
    }

    shutdown(subsys, worterbuch, config, servers).await
}

async fn run_with_leader(
    subsys: &SubsystemHandle,
    worterbuch: &mut Worterbuch,
    api_rx: &mut mpsc::Receiver<WbFunction>,
    config: Config,
    leader_address: SocketAddr,
) -> WorterbuchAppResult<bool> {
    let mut persistence_interval = config.persistence_interval();

    let stream = match TcpStream::connect(leader_address).await {
        Ok(it) => it,
        Err(e) => {
            warn!("Failed to connect to leader {}: {}", leader_address, e);
            return Ok(false);
        }
    };
    let mut lines = BufReader::new(stream).lines();

    let timeout = config.initial_sync_timeout;

    info!("Successfully connected to leader {leader_address}. Waiting for initial sync message …");
    select! {
        biased;
        _ = subsys.shutdown_requested() => {
            warn!("Shutdown requested before initial sync completed.");
            return Err(WorterbuchAppError::ClusterError("shut down before initial sync".to_owned()));
        },
        recv = receive_msg(&mut lines, timeout) => {
            debug!("Received leader message");
            match recv {
                Ok(Some(msg)) => {
                    if let LeaderSyncMessage::Init(state) = msg {
                        debug!("Received initial sync message from leader: {state:?}");
                        initial_sync(state, worterbuch).await?;
                        persistence_interval.reset();
                        worterbuch.flush().await?;
                    } else {
                        warn!("Expected initial sync message from leader, but got: {msg:?}");
                        return Ok(false);
                    }
                },
                Ok(None) => {
                    warn!("Leader closed connection before sending initial sync message.");
                    return Ok(false);
                },
                Err(e) => {
                    warn!("Error receiving initial sync message from leader: {e}");
                    return Ok(false);
                }
            }
        },
    }
    info!("Successfully synced with leader.");

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = receive_msg(&mut lines, None) => try_process_leader_message(recv, worterbuch).await?,
        recv = api_rx.recv() => try_process_api_call(recv, worterbuch).await?,
    }

    Ok(true)
}

async fn initial_sync(
    state_sync: StateSync,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
    // TODO create diff with current state store
    // TODO send out diff to all clients

    worterbuch.reset_store(state_sync.store).await?;
    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Follower),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::LeaderSync),
            true,
        )
        .await?;

    unlock_persistence();

    worterbuch.flush().await.map_err(|e| {
        WorterbuchAppError::ClusterError(format!("Failed to flush storage after initial sync: {e}"))
    })?;
    Ok(())
}

async fn try_process_api_call(
    recv: Option<WbFunction>,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Some(function) => {
            process_api_call(worterbuch, function).await;
            Ok(ControlFlow::Continue(()))
        }
        None => Ok(ControlFlow::Break(())),
    }
}

async fn process_api_call(_worterbuch: &mut Worterbuch, _function: WbFunction) {
    // TODO forward to leader
}
