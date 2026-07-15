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
    cluster::{self, protocol::ProxyMessage},
    cluster::{
        Mode,
        follower::try_process_leader_message,
        protocol::{LeaderMessage, StateSync},
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
    net::tcp::OwnedWriteHalf,
    select,
    sync::mpsc,
};
use tosub::SubsystemHandle;
use tracing::{debug, error, info, warn};
use worterbuch_common::{
    INTERNAL_CLIENT_ID,
    error::ConfigError,
    protocol::{ClientMessage, Set},
    protocol::{InternalAction, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, Trace},
    receive_msg, topic, while_select, write_line_and_flush,
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

    info!("Main loop stopped, shutting down.");

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
    let (leader_rx, leader_tx) = stream.into_split();
    let mut lines = BufReader::new(leader_rx).lines();

    let proxy_request_sender = init_request_sender(subsys, leader_tx, &config, leader_address);

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
                    if let LeaderMessage::Init(state) = msg {
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

    // TODO delay opening of client sockets until after initial sync

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = receive_msg(&mut lines, None) => try_process_leader_message(recv, worterbuch).await?,
        recv = api_rx.recv() => try_process_api_call(recv, worterbuch, &proxy_request_sender).await?,
    }

    info!(
        "Proxy loop for leader {} stopped, closing connection.",
        leader_address
    );

    Ok(true)
}

fn init_request_sender(
    subsys: &SubsystemHandle,
    leader_tx: OwnedWriteHalf,
    config: &Config,
    leader_addr: SocketAddr,
) -> mpsc::Sender<ProxyMessage> {
    let (tx, rx) = mpsc::channel(config.channel_buffer_size);
    let send_timeout = config.send_timeout;
    subsys.spawn("proxy_request_sender", move |s| {
        request_sneder_loop(s, leader_tx, rx, send_timeout, leader_addr)
    });
    tx
}

async fn request_sneder_loop(
    subsys: SubsystemHandle,
    mut leader_tx: OwnedWriteHalf,
    mut rx: mpsc::Receiver<ProxyMessage>,
    timeout: Option<Duration>,
    leader_addr: SocketAddr,
) -> miette::Result<()> {
    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = rx.recv() => forward_client_request(recv, &mut leader_tx, timeout, leader_addr).await,
    }
    Ok(())
}

async fn forward_client_request(
    recv: Option<ProxyMessage>,
    leader_tx: &mut OwnedWriteHalf,
    timeout: Option<Duration>,
    leader_addr: SocketAddr,
) -> ControlFlow<()> {
    let Some(request) = recv else {
        return ControlFlow::Break(());
    };

    debug!("Forwarding client request to leader");

    if let Err(e) = write_line_and_flush(request, leader_tx, timeout, leader_addr).await {
        error!(
            "Failed to forward client request to leader {}: {e}",
            leader_addr
        );
        return ControlFlow::Break(());
    }

    ControlFlow::Continue(())
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
    leader_tx: &mpsc::Sender<ProxyMessage>,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Some(function) => {
            process_api_call(worterbuch, function, leader_tx).await?;
            Ok(ControlFlow::Continue(()))
        }
        None => Ok(ControlFlow::Break(())),
    }
}

async fn process_api_call(
    worterbuch: &mut Worterbuch,
    function: WbFunction,
    leader_tx: &mpsc::Sender<ProxyMessage>,
) -> WorterbuchAppResult<()> {
    match function {
        WbFunction::Connected(client_id, addr, protocol, tx) => {
            let request = ProxyMessage::Connected {
                client_id,
                protocol: protocol.clone(),
            };
            // TODO register response interest?
            leader_tx.send(request).await?;
            cluster::process_api_call(
                worterbuch,
                WbFunction::Connected(client_id, addr, protocol, tx),
            )
            .await;
        }
        WbFunction::Disconnected(client_id, protocol, tx) => {
            let request = ProxyMessage::Disconnected {
                client_id,
                protocol: protocol.clone(),
            };
            // TODO register response interest?
            leader_tx.send(request).await?;
            cluster::process_api_call(
                worterbuch,
                WbFunction::Disconnected(client_id, protocol, tx),
            )
            .await;
        }
        WbFunction::ProtocolSwitched(client_id, interface, version) => {
            let request = ProxyMessage::ProtocolSwitched {
                client_id,
                interface: interface.clone(),
                version,
            };
            // TODO register response interest?
            leader_tx.send(request).await?;
            cluster::process_api_call(
                worterbuch,
                WbFunction::ProtocolSwitched(client_id, interface, version),
            )
            .await;
        }
        WbFunction::Set(transaction_id, interface, key, value, client_id, tx, span) => {
            let request = ProxyMessage::Request {
                client_id,
                msg: ClientMessage::Set(Set {
                    transaction_id,
                    key,
                    value,
                }),
                interface,
            };
            // TODO register response interest
            leader_tx.send(request).await?;
        }
        WbFunction::CSet(_, _, _, _, _, _, _) => {
            warn!("CSet not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::SPubInit(_, _, _, _, _) => {
            warn!("SPubInit not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::SPub(_, _, _, _) => {
            warn!("SPub not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::Publish(_, _, _, _, _, _) => {
            warn!("Publish not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::Delete(_, _, _, _, _) => {
            warn!("Delete not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::PDelete(_, _, _, _, _) => {
            warn!("PDelete not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::Lock(_, _, _, _, _) => {
            warn!("Lock not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::AcquireLock(_, _, _, _, _) => {
            warn!("AcquireLock not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::ReleaseLock(_, _, _, _, _) => {
            warn!("ReleaseLock not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        WbFunction::Import(_, _, _, _, _) => {
            warn!("Import not yet implemented");
            // TODO forward to leader
            // TODO register response interest
        }
        function => cluster::process_api_call(worterbuch, function).await,
    };

    Ok(())
}
