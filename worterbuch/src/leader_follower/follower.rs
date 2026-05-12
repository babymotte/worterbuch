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
    Config, INTERNAL_CLIENT_ID, Worterbuch,
    error::{WorterbuchAppError, WorterbuchAppResult},
    leader_follower::{ClientWriteCommand, LeaderSyncMessage, Mode, StateSync},
    persistence::unlock_persistence,
    server::common::WbFunction,
    shutdown,
};
use serde_json::json;
use std::ops::ControlFlow;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    select,
    sync::mpsc,
};
use tosub::SubsystemHandle;
use tracing::{debug, error, info, trace};
use worterbuch_common::{
    SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT,
    error::{ConnectionResult, WorterbuchError},
    receive_msg, topic, while_select,
};

pub(crate) async fn run_in_follower_mode(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    web_server: Option<SubsystemHandle>,
) -> WorterbuchAppResult<()> {
    let leader_addr = if let Some(it) = &config.leader_address {
        it
    } else {
        return Err(WorterbuchAppError::ConfigError(
            "No valid leader address configured.".to_owned(),
        ));
    };
    info!("Running in FOLLOWER mode. Leader: {}", leader_addr,);

    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Follower),
            INTERNAL_CLIENT_ID,
            true,
        )
        .await?;

    let mut persistence_interval = config.persistence_interval();

    let stream = TcpStream::connect(leader_addr).await?;

    let mut lines = BufReader::new(stream).lines();

    info!("Waiting for initial sync message from leader …");
    select! {
        recv = receive_msg(&mut lines) => match recv {
            Ok(Some(msg)) => {
                if let LeaderSyncMessage::Init(state) = msg {
                    initial_sync(state, &mut worterbuch).await?;
                    unlock_persistence();
                    persistence_interval.reset();
                    worterbuch.flush().await?;
                } else {
                    return Err(WorterbuchAppError::ClusterError("first message from leader is supposed to be the initial sync, but it wasn't".to_owned()));
                }
            },
            Ok(None) => return Err(WorterbuchAppError::ClusterError("connection to leader closed before initial sync".to_owned())),
            Err(e) => {
                return Err(WorterbuchAppError::ClusterError(format!("error receiving update from leader: {e}")));
            }
        },
        _ = subsys.shutdown_requested() => return Err(WorterbuchAppError::ClusterError("shut down before initial sync".to_owned())),
    }
    info!("Successfully synced with leader.");

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        _ = persistence_interval.tick() => try_flush(&mut worterbuch).await?,
        recv = receive_msg(&mut lines) => try_process_leader_message(recv, &mut worterbuch).await?,
        recv = api_rx.recv() => try_process_api_call(recv, &mut worterbuch).await?,
    }

    shutdown(subsys, worterbuch, config, web_server, None, None).await
}

async fn try_process_leader_message(
    recv: ConnectionResult<Option<LeaderSyncMessage>>,
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

async fn try_flush(worterbuch: &mut Worterbuch) -> WorterbuchAppResult<ControlFlow<()>> {
    debug!("Follower persistence interval triggered");
    worterbuch.flush().await?;
    Ok(ControlFlow::Continue(()))
}

async fn initial_sync(
    state_sync: StateSync,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
    worterbuch.reset_store(state_sync.0).await?;
    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Follower),
            INTERNAL_CLIENT_ID,
            true,
        )
        .await?;
    Ok(())
}

async fn process_leader_message(
    msg: LeaderSyncMessage,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
    trace!("Received leader sync message: {msg:?}");

    let res = match msg {
        LeaderSyncMessage::Init(_) => {
            return Err(crate::error::WorterbuchAppError::ClusterError(
                "already synced".to_owned(),
            ));
        }
        LeaderSyncMessage::Mut(client_write_command) => match client_write_command {
            ClientWriteCommand::Set(key, value, force) => {
                worterbuch.set(key, value, INTERNAL_CLIENT_ID, force).await
            }
            ClientWriteCommand::CSet(key, value, versions, force) => {
                worterbuch
                    .cset(key, value, versions, INTERNAL_CLIENT_ID, force)
                    .await
            }
            ClientWriteCommand::Delete(key) => {
                worterbuch.delete(key, INTERNAL_CLIENT_ID).await.map(|_| ())
            }
            ClientWriteCommand::PDelete(pattern) => worterbuch
                .pdelete(pattern, INTERNAL_CLIENT_ID)
                .await
                .map(|_| ()),
        },
    };

    if let Err(e) = res {
        error!("Error applying leader sync message: {e}");
    }

    Ok(())
}

async fn process_api_call(worterbuch: &mut Worterbuch, function: WbFunction) {
    match function {
        WbFunction::Get(key, tx) => {
            tx.send(worterbuch.get(&key)).ok();
        }
        WbFunction::CGet(key, tx) => {
            tx.send(worterbuch.cget(&key)).ok();
        }
        WbFunction::Set(_, _, _, tx, _) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::CSet(_, _, _, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::SPubInit(_, _, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::SPub(_, _, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::Publish(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::Ls(parent, tx) => {
            tx.send(worterbuch.ls(&parent)).ok();
        }
        WbFunction::PLs(parent, tx) => {
            tx.send(worterbuch.pls(&parent)).ok();
        }
        WbFunction::PGet(pattern, tx) => {
            tx.send(worterbuch.pget(&pattern)).ok();
        }
        WbFunction::Subscribe(client_id, transaction_id, key, unique, live_only, tx) => {
            tx.send(
                worterbuch
                    .subscribe(client_id, transaction_id, key, unique, live_only)
                    .await,
            )
            .ok();
        }
        WbFunction::PSubscribe(client_id, transaction_id, pattern, unique, live_only, tx) => {
            tx.send(
                worterbuch
                    .psubscribe(client_id, transaction_id, pattern, unique, live_only)
                    .await,
            )
            .ok();
        }
        WbFunction::SubscribeLs(client_id, transaction_id, parent, tx) => {
            tx.send(
                worterbuch
                    .subscribe_ls(client_id, transaction_id, parent)
                    .await,
            )
            .ok();
        }
        WbFunction::Unsubscribe(client_id, transaction_id, tx) => {
            tx.send(worterbuch.unsubscribe(client_id, transaction_id).await)
                .ok();
        }
        WbFunction::UnsubscribeLs(client_id, transaction_id, tx) => {
            tx.send(worterbuch.unsubscribe_ls(client_id, transaction_id))
                .ok();
        }
        WbFunction::Lock(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::AcquireLock(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::ReleaseLock(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::Delete(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::PDelete(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::Connected(client_id, remote_addr, protocol, tx) => {
            let res = worterbuch
                .connected(client_id, remote_addr, &protocol)
                .await;
            tx.send(res).ok();
        }
        WbFunction::ProtocolSwitched(client_id, protocol) => {
            worterbuch.protocol_switched(client_id, protocol).await;
        }
        WbFunction::Disconnected(client_id, remote_addr) => {
            worterbuch.disconnected(client_id, remote_addr).await.ok();
        }
        WbFunction::Config(tx) => {
            tx.send(worterbuch.config().clone()).ok();
        }
        WbFunction::Export(tx, span) => {
            _ = span.enter();
            worterbuch.export_for_persistence(tx);
        }
        WbFunction::Import(_, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::Len(tx) => {
            tx.send(worterbuch.len()).ok();
        }
    }
}
