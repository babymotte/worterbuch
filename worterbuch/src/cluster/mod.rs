/*
 *  Types and helper functions for cluster mode
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

pub(crate) mod follower;
pub(crate) mod leader;
pub mod protocol;
pub(crate) mod proxy;
pub(crate) mod standalone;

use crate::{
    Config, Servers,
    cluster::protocol::ClusterStateChange,
    error::WorterbuchAppResult,
    server::common::WbFunction,
    worterbuch::{SubscriptionFlags, Worterbuch},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tosub::Subsystem;
use tracing::{Instrument, Level, info, instrument, trace};
use worterbuch_common::protocol::v1::{InternalAction, Trace, TraceData};

pub type ClusterStateChangeReceiver = mpsc::Receiver<ClusterStateChange>;
pub type ClusterStateChangeSender = mpsc::Sender<ClusterStateChange>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mode {
    Standalone,
    Leader,
    Follower,
    Proxy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaderState {
    Disconnected,
    Connecting(String),
    Handshake(String),
    Syncing(String),
    Synced(String),
}

#[instrument(level = Level::TRACE, skip_all)]
async fn process_api_call(worterbuch: &mut Worterbuch, function: WbFunction) {
    trace!(enter = "process_api_call");
    match function {
        WbFunction::Get(req, _client_info, tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.get(&req.key);
            let _ = tx.send(res);
        }
        WbFunction::CGet(req, _client_info, tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.cget(&req.key);
            let _ = tx.send(res);
        }
        WbFunction::Set(req, client_info, tx, span) => {
            let res = worterbuch
                .set(
                    req.key,
                    req.value,
                    false,
                    TraceData::new(req.transaction_id, client_info),
                )
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::CSet(req, client_info, tx, span) => {
            let res = worterbuch
                .cset(
                    req.key,
                    req.value,
                    req.version,
                    false,
                    TraceData::new(req.transaction_id, client_info),
                )
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::SPubInit(req, client_info, tx, span) => {
            let res = worterbuch
                .spub_init(req.key, TraceData::new(req.transaction_id, client_info))
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::SPub(req, client_info, tx, span) => {
            let res = worterbuch
                .spub(
                    req.value,
                    client_info.client_id,
                    TraceData::new(req.transaction_id, client_info),
                )
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::Publish(req, client_info, tx, span) => {
            let res = worterbuch
                .publish(
                    req.key,
                    req.value,
                    client_info.client_id,
                    TraceData::new(req.transaction_id, client_info),
                )
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::Ls(req, _client_info, tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.ls(&req.parent);
            let _ = tx.send(res);
        }
        WbFunction::PLs(req, _client_info, tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.pls(&req.parent_pattern);
            let _ = tx.send(res);
        }
        WbFunction::PGet(req, _client_info, tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.pget(&req.request_pattern);
            let _ = tx.send(res);
        }
        WbFunction::Subscribe(req, client_info, tx, span) => {
            let flags = SubscriptionFlags::from(&req);
            let res = worterbuch
                .subscribe(
                    req.key,
                    flags,
                    TraceData::new(req.transaction_id, client_info),
                )
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::PSubscribe(req, client_info, tx, span) => {
            let flags = SubscriptionFlags::from(&req);
            let res = worterbuch
                .psubscribe(
                    req.request_pattern,
                    flags,
                    TraceData::new(req.transaction_id, client_info),
                )
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::SubscribeLs(req, client_info, tx, span) => {
            let res = worterbuch
                .subscribe_ls(
                    req.parent,
                    req.send_traces.unwrap_or(false),
                    TraceData::new(req.transaction_id, client_info),
                )
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::Unsubscribe(req, client_info, tx, span) => {
            let res = worterbuch
                .unsubscribe(TraceData::new(req.transaction_id, client_info))
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::UnsubscribeLs(req, client_info, tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.unsubscribe_ls(client_info.client_id, req.transaction_id);
            let _ = tx.send(res);
        }
        WbFunction::Delete(req, client_info, tx, span) => {
            let res = worterbuch
                .delete(req.key, req.transaction_id, client_info)
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::PDelete(req, client_info, tx, span) => {
            let res = worterbuch
                .pdelete(req.request_pattern, req.transaction_id, client_info)
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::Lock(req, client_info, tx, span) => {
            trace!(client_id = %client_info.client_id, key = req.key, "lock");
            let res = worterbuch
                .lock(req.key, req.transaction_id, client_info)
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::AcquireLock(req, client_info, tx, span) => {
            trace!(client_id = %client_info.client_id, key = req.key, "acquire_lock");
            let res = worterbuch
                .acquire_lock(req.key, req.transaction_id, client_info)
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::ReleaseLock(req, client_info, tx, span) => {
            trace!(client_id = %client_info.client_id, key = req.key, "release_lock");
            let res = worterbuch
                .release_lock(req.key, req.transaction_id, client_info)
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::Connected(client_info, remote_addr, eject, tx, span) => {
            trace!(client_id = %client_info.client_id, "connected");
            let res = worterbuch
                .connected(client_info, remote_addr, eject)
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::ProtocolSwitched(client_info, protocol, span) => {
            trace!(client_id = %client_info.client_id, protocol, "protocol_switched");
            let _ = worterbuch
                .protocol_switched(client_info, protocol)
                .instrument(span)
                .await;
        }
        WbFunction::Disconnected(client_info, remote_addr, span) => {
            let client_id = client_info.client_id;
            trace!(%client_id, "disconnected");
            let _ = worterbuch
                .disconnected(client_info, remote_addr)
                .instrument(span)
                .await;
        }
        WbFunction::Config(tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.config().to_owned();
            let _ = tx.send(res);
        }
        WbFunction::Export(tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.export_for_persistence();
            let _ = tx.send(res);
        }
        WbFunction::Import(transaction_id, client_info, json, tx, span) => {
            let res = worterbuch
                .import(&json, transaction_id, client_info)
                .instrument(span)
                .await;
            let _ = tx.send(res);
        }
        WbFunction::Len(tx, span) => {
            let _guard = span.enter();
            let res = worterbuch.len();
            let _ = tx.send(res);
        }
        WbFunction::Tick(span) => {
            worterbuch.tick().instrument(span).await;
        }
        WbFunction::ReGrantLocks(locks, tx, span) => {
            let res = worterbuch.re_grant_locks(locks).instrument(span).await;
            let _ = tx.send(res);
        }
    }
    trace!(exit = "process_api_call");
}

#[instrument(level = Level::TRACE, skip_all)]
async fn shutdown(
    subsys: &Subsystem,
    mut worterbuch: Worterbuch,
    config: Config,
    servers: Servers,
) -> WorterbuchAppResult<()> {
    info!("Shutdown sequence triggered");

    subsys.request_global_shutdown_because("Shutdown requested");

    shutdown_servers(servers).await;

    if config.use_persistence {
        info!("Applying grave goods and last wills …");
        worterbuch
            .apply_all_grave_goods_and_last_wills(Trace::InternalAction(InternalAction::Shutdown))
            .await;
        info!("Waiting for persistence hook to complete …");
        worterbuch.flush().await?;
        info!("Shutdown persistence hook complete.");
    }

    Ok(())
}

#[instrument(level = Level::TRACE, skip_all)]
async fn shutdown_servers(servers: Servers) {
    if let Some(it) = servers.web_server {
        info!("Shutting down web server …");
        it.request_local_shutdown_because("shutting down servers");
        let _ = it.join().await;
    }

    if let Some(it) = servers.tcp_server {
        info!("Shutting down tcp server …");
        it.request_local_shutdown_because("shutting down servers");
        let _ = it.join().await;
    }

    if let Some(it) = servers.unix_socket {
        info!("Shutting down unix socket …");
        it.request_local_shutdown_because("shutting down servers");
        let _ = it.join().await;
    }

    if let Some(it) = servers.quic_server {
        info!("Shutting down QUIC server …");
        it.request_local_shutdown_because("shutting down servers");
        let _ = it.join().await;
    }
}
