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
            ClientWriteCommand, ClusterStateChange, LeaderMessage, LeaderWelcome, StateSync,
        },
        shutdown,
    },
    error::{WorterbuchAppError, WorterbuchAppResult},
    persistence::unlock_persistence,
};
use serde_json::json;
use std::ops::ControlFlow;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    select,
};
use tosub::SubsystemHandle;
use tracing::{debug, error, info, trace, warn};
use worterbuch_common::{
    error::ConnectionResult,
    protocol::v1::{InternalAction, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, Trace},
    receive_msg, topic, while_select,
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
                    if let LeaderMessage::Init(state) = msg {
                        debug!("Received initial sync message from leader: {state:?}");
                        initial_sync(state, &mut worterbuch).await?;
                        persistence_interval.reset();
                        worterbuch.flush().await?;
                    } else {
                        return Err(WorterbuchAppError::ClusterError("first message from leader is supposed to be the initial sync, but it wasn't".to_owned()));
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
        LeaderMessage::Welcome(LeaderWelcome {
            version,
            authentication_required,
        }) => {
            // TODO send handshake
            Ok(())
        }
        LeaderMessage::Init(_) => {
            return Err(crate::error::WorterbuchAppError::ClusterError(
                "already synced".to_owned(),
            ));
        }
        LeaderMessage::Mut(ClusterStateChange { command, trace, .. }) => match command {
            ClientWriteCommand::Set(key, value, force) => {
                worterbuch
                    .internal_set(key, value, INTERNAL_CLIENT_ID, trace, force)
                    .await
            }
            ClientWriteCommand::CSet(key, value, versions, force) => {
                worterbuch
                    .internal_cset(key, value, versions, INTERNAL_CLIENT_ID, trace, force)
                    .await
            }
            ClientWriteCommand::Delete(key) => worterbuch
                .internal_delete(key, INTERNAL_CLIENT_ID, trace)
                .await
                .map(|_| ()),
            ClientWriteCommand::PDelete(pattern) => worterbuch
                .internal_pdelete(pattern, false, INTERNAL_CLIENT_ID, trace)
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
