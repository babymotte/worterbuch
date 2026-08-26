/*
 *  Helper functions for follower mode
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
    Config, INTERNAL_CLIENT_ID, Servers, Worterbuch,
    cluster::{
        Mode,
        protocol::{
            ClientWriteCommand, ClusterStateChange, FollowerHandshake, Handshake, LeaderMessage,
            LeaderWelcome, ProxyMessage, StateSync,
        },
        shutdown,
    },
    error::{WorterbuchAppError, WorterbuchAppResult},
    persistence::unlock_persistence,
    worterbuch_version,
};
use serde_json::json;
use std::{net::SocketAddr, ops::ControlFlow, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::{TcpStream, tcp::OwnedWriteHalf},
    select,
    sync::mpsc,
};
use tosub::SubsystemHandle;
use tracing::{debug, error, info, trace, warn};
use worterbuch_common::{
    error::ConnectionResult,
    protocol::v1::{InternalAction, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, Trace},
    receive_msg, topic, while_select, write_line_and_flush,
};

pub(crate) async fn run(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    config: Config,
    web_server: Option<SubsystemHandle>,
    leader_address: String,
) -> WorterbuchAppResult<()> {
    #[cfg(feature = "commercial")]
    if !config.license.features.clustering {
        return Err(crate::error::WorterbuchAppError::NoLicense(
            "clustering".to_owned(),
        ));
    }

    info!("Running in FOLLOWER mode. Leader: {}", leader_address,);

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Follower),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::Startup),
            true,
        )
        .await?;

    let mut persistence_interval = config.persistence_interval();

    let stream = TcpStream::connect(&leader_address).await?;
    let leader_address = stream.peer_addr()?;

    let (leader_rx, leader_tx) = stream.into_split();
    let mut lines = BufReader::new(leader_rx).lines();

    let follower_request_tx = init_request_sender(subsys, leader_tx, &config, leader_address);

    let timeout = config.initial_sync_timeout;

    info!("Successfully connected to leader {leader_address}. Waiting for initial sync message …");

    let welcome = select! {
        biased;
        _ = subsys.shutdown_requested() => {
            warn!("Shutdown requested before receiving leader welcome message.");
            return Err(WorterbuchAppError::ClusterError("shut down before receiving leader welcome message".to_owned()));
        },
        recv = receive_msg(&mut lines, timeout) => {
            debug!("Received leader message");
            match recv {
                Ok(Some(msg)) => {
                    if let LeaderMessage::Welcome(welcome) = msg {
                        debug!("Received welcome message from leader: {welcome:?}");
                        welcome
                    } else {
                        warn!("Expected welcome message from leader, but got: {msg:?}");
                        return Err(WorterbuchAppError::ClusterError(format!("Expected welcome message from leader, but got: {msg:?}")));
                    }
                },
                Ok(None) => {
                    warn!("Leader closed connection before sending welcome message.");
                    return Err(WorterbuchAppError::ClusterError("Leader closed connection before sending welcome message.".to_owned()));
                },
                Err(e) => {
                    warn!("Error receiving welcome message from leader: {e}");
                    return Err(WorterbuchAppError::ClusterError(format!("Error receiving welcome message from leader: {e}")));
                }
            }
        },
    };

    // TODO check version
    send_handshake(welcome, &worterbuch, &config, &follower_request_tx).await?;

    info!("Handshake complete. Waiting for initial sync message …");

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
                        initial_sync(state, &mut worterbuch).await?;
                        persistence_interval.reset();
                        worterbuch.flush().await?;
                    } else {
                        return Err(WorterbuchAppError::ClusterError(format!("Expected initial sync, but it got: {msg:?}")));
                    }
                },
                Ok(None) => return Err(WorterbuchAppError::ClusterError("connection to leader closed before initial sync".to_owned())),
                Err(e) => {
                    return Err(WorterbuchAppError::ClusterError(format!("error receiving initial sync message from leader: {e}")));
                }
            }
        },
    }
    info!("Successfully synced with leader.");

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        _ = persistence_interval.tick() => try_flush(&mut worterbuch).await?,
        recv = receive_msg(&mut lines, None) => try_process_leader_message(recv, &mut worterbuch).await?,
    }

    info!("Main loop stopped, shutting down.");

    shutdown(
        subsys,
        worterbuch,
        config,
        Servers {
            web_server,
            ..Default::default()
        },
    )
    .await
}

async fn send_handshake(
    welcome: LeaderWelcome,
    worterbuch: &Worterbuch,
    config: &Config,
    follower_request_tx: &mpsc::Sender<ProxyMessage>,
) -> WorterbuchAppResult<()> {
    let version = worterbuch_version();
    let auth_token = if welcome.authentication_required {
        todo!()
    } else {
        None
    };

    let handshake = ProxyMessage::Handshake(Handshake::Follower(FollowerHandshake {
        version,
        auth_token,
    }));

    follower_request_tx.send(handshake).await?;

    Ok(())
}

async fn try_process_leader_message(
    recv: ConnectionResult<Option<LeaderMessage>>,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<ControlFlow<()>> {
    match recv {
        Ok(Some(msg)) => {
            process_leader_message(msg, worterbuch).await?;
            Ok(ControlFlow::Continue(()))
        }
        Ok(None) => Ok(ControlFlow::Break(())),
        Err(e) => {
            error!("Error receiving update from leader: {e}");
            Ok(ControlFlow::Break(()))
        }
    }
}

async fn try_flush(worterbuch: &mut Worterbuch) -> WorterbuchAppResult<ControlFlow<()>> {
    debug!("Follower persistence interval triggered");
    worterbuch.flush().await?;
    Ok(ControlFlow::Continue(()))
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
        request_sender_loop(s, leader_tx, rx, send_timeout, leader_addr)
    });
    tx
}

async fn request_sender_loop(
    subsys: SubsystemHandle,
    mut leader_tx: OwnedWriteHalf,
    mut rx: mpsc::Receiver<ProxyMessage>,
    timeout: Option<Duration>,
    leader_addr: SocketAddr,
) -> miette::Result<()> {
    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = rx.recv() => forward_client_request(&subsys, recv, &mut leader_tx, timeout, leader_addr).await,
    }
    Ok(())
}

async fn forward_client_request(
    subsys: &SubsystemHandle,
    recv: Option<ProxyMessage>,
    leader_tx: &mut OwnedWriteHalf,
    timeout: Option<Duration>,
    leader_addr: SocketAddr,
) -> ControlFlow<()> {
    let Some(request) = recv else {
        return ControlFlow::Break(());
    };

    debug!("Forwarding client request to leader: {request:?}");

    if let Err(e) = write_line_and_flush(
        || subsys.shutdown_requested(),
        request,
        leader_tx,
        timeout,
        leader_addr,
    )
    .await
    {
        error!(
            "Failed to forward client request to leader {}: {e}",
            leader_addr
        );
        return ControlFlow::Break(());
    }

    trace!("Client request forwarded to leader.");

    ControlFlow::Continue(())
}

async fn initial_sync(
    state_sync: StateSync,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
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

async fn process_leader_message(
    msg: LeaderMessage,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
    trace!("Received leader sync message: {msg:?}");

    let res = match msg {
        LeaderMessage::Welcome(_) => {
            return Err(crate::error::WorterbuchAppError::ClusterError(
                "already received welcome message".to_owned(),
            ));
        }
        LeaderMessage::Init(_) => {
            return Err(crate::error::WorterbuchAppError::ClusterError(
                "already synced".to_owned(),
            ));
        }
        LeaderMessage::Mut(ClusterStateChange { command, trace, .. }) => match command {
            ClientWriteCommand::Set(key, value, force) => {
                worterbuch
                    .internal_set(
                        key,
                        value,
                        trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                        trace,
                        force,
                    )
                    .await
            }
            ClientWriteCommand::CSet(key, value, versions, force) => {
                worterbuch
                    .internal_cset(
                        key,
                        value,
                        versions,
                        trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                        trace,
                        force,
                    )
                    .await
            }
            ClientWriteCommand::Delete(key) => worterbuch
                .internal_delete(key, trace.client_id().unwrap_or(INTERNAL_CLIENT_ID), trace)
                .await
                .map(|_| ()),
            ClientWriteCommand::PDelete(pattern) => worterbuch
                .internal_pdelete(
                    pattern,
                    trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                    trace,
                )
                .await
                .map(|_| ()),
            ClientWriteCommand::Publish(_, _) => {
                panic!("leader should never forward a Publish command to a follower")
            }
            ClientWriteCommand::Import(persisted_store) => worterbuch
                .internal_import(
                    persisted_store,
                    trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                    trace,
                )
                .await
                .map(|_| ()),
        },
        LeaderMessage::ClientResponse(_, _) => {
            return Err(crate::error::WorterbuchAppError::ClusterError(
                "leader should never send a ClientResponse to a follower".to_owned(),
            ));
        }
    };

    if let Err(e) = res {
        error!("Error applying leader sync message: {e}");
    }

    Ok(())
}
