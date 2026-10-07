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
pub(crate) mod protocol;
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
use tracing::{Instrument, info, trace};
use worterbuch_common::protocol::v1::{InternalAction, Trace, TraceData};

pub type ClusterStateChangeReceiver = mpsc::Receiver<ClusterStateChange>;
pub type ClusterStateChangeSender = mpsc::Sender<ClusterStateChange>;

#[derive(Serialize)]
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

async fn process_api_call(worterbuch: &mut Worterbuch, function: WbFunction) {
    trace!(enter = "process_api_call");
    match function {
        WbFunction::Get(key, tx) => {
            let _ = tx.send(worterbuch.get(&key));
        }
        WbFunction::CGet(key, tx) => {
            let _ = tx.send(worterbuch.cget(&key));
        }
        WbFunction::Set(transaction_id, interface, key, value, client_id, tx, span) => {
            let _ = tx.send(
                worterbuch
                    .set(
                        key,
                        value,
                        false,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .instrument(span)
                    .await,
            );
        }
        WbFunction::CSet(transaction_id, interface, key, value, version, client_id, tx) => {
            let _ = tx.send(
                worterbuch
                    .cset(
                        key,
                        value,
                        version,
                        false,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            );
        }
        WbFunction::SPubInit(transaction_id, interface, key, client_id, tx) => {
            let _ = tx.send(
                worterbuch
                    .spub_init(key, TraceData::new(client_id, interface, transaction_id))
                    .await,
            );
        }
        WbFunction::SPub(transaction_id, interface, value, client_id, tx) => {
            let _ = tx.send(
                worterbuch
                    .spub(
                        value,
                        client_id,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            );
        }
        WbFunction::Publish(transaction_id, interface, key, value, client_id, tx) => {
            let _ = tx.send(
                worterbuch
                    .publish(
                        key,
                        value,
                        client_id,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            );
        }
        WbFunction::Ls(parent, tx) => {
            let _ = tx.send(worterbuch.ls(&parent));
        }
        WbFunction::PLs(parent, tx) => {
            let _ = tx.send(worterbuch.pls(&parent));
        }
        WbFunction::PGet(pattern, tx) => {
            let _ = tx.send(worterbuch.pget(&pattern));
        }
        WbFunction::Subscribe(
            client_id,
            transaction_id,
            interface,
            key,
            unique,
            live_only,
            send_traces,
            tx,
        ) => {
            let _ = tx.send(
                worterbuch
                    .subscribe(
                        key,
                        SubscriptionFlags::new(unique, live_only, send_traces),
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            );
        }
        WbFunction::PSubscribe(
            client_id,
            transaction_id,
            interface,
            pattern,
            unique,
            live_only,
            send_traces,
            tx,
        ) => {
            let _ = tx.send(
                worterbuch
                    .psubscribe(
                        pattern,
                        SubscriptionFlags::new(unique, live_only, send_traces),
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            );
        }
        WbFunction::SubscribeLs(client_id, transaction_id, interface, parent, send_traces, tx) => {
            let _ = tx.send(
                worterbuch
                    .subscribe_ls(
                        parent,
                        send_traces,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            );
        }
        WbFunction::Unsubscribe(client_id, transaction_id, interface, tx) => {
            let _ = tx.send(
                worterbuch
                    .unsubscribe(TraceData::new(client_id, interface, transaction_id))
                    .await,
            );
        }
        WbFunction::UnsubscribeLs(client_id, transaction_id, tx) => {
            let _ = tx.send(worterbuch.unsubscribe_ls(client_id, transaction_id));
        }
        WbFunction::Delete(transaction_id, interface, key, client_id, tx) => {
            let _ = tx.send(
                worterbuch
                    .delete(key, client_id, transaction_id, interface)
                    .await,
            );
        }
        WbFunction::PDelete(transaction_id, interface, pattern, _, client_id, tx) => {
            let _ = tx.send(
                worterbuch
                    .pdelete(pattern, client_id, transaction_id, interface)
                    .await,
            );
        }
        WbFunction::Lock(transaction_id, interface, key, client_id, tx) => {
            trace!(%client_id, key, "lock");
            let _ = tx.send(
                worterbuch
                    .lock(key, client_id, transaction_id, interface)
                    .await,
            );
        }
        WbFunction::AcquireLock(transaction_id, interface, key, client_id, tx) => {
            trace!(%client_id, key, "acquire_lock");
            let _ = tx.send(
                worterbuch
                    .acquire_lock(key, client_id, transaction_id, interface)
                    .await,
            );
        }
        WbFunction::ReleaseLock(transaction_id, interface, key, client_id, tx) => {
            trace!(%client_id, key, "release_lock");
            let _ = tx.send(
                worterbuch
                    .release_lock(key, client_id, transaction_id, interface)
                    .await,
            );
        }
        WbFunction::Connected(client_id, remote_addr, protocol, eject, tx) => {
            trace!(%client_id, "connected");
            let _ = tx.send(
                worterbuch
                    .connected(client_id, remote_addr, protocol, eject)
                    .await,
            );
        }
        WbFunction::ProtocolSwitched(client_id, interface, protocol) => {
            trace!(%client_id, protocol, "protocol_switched");
            let _ = worterbuch
                .protocol_switched(client_id, interface, protocol)
                .await;
        }
        WbFunction::Disconnected(client_id, protocol, remote_addr) => {
            trace!(%client_id, "disconnected");
            let _ = worterbuch
                .disconnected(client_id, protocol, remote_addr)
                .await;
        }
        WbFunction::Config(tx) => {
            let _ = tx.send(worterbuch.config().clone());
        }
        WbFunction::Export(tx, span) => {
            let g = span.enter();
            worterbuch.export_for_persistence(tx);
            drop(g);
            drop(span);
        }
        WbFunction::Import(transaction_id, client_id, interface, json, tx) => {
            let _ = tx.send(
                worterbuch
                    .import(&json, client_id, transaction_id, interface)
                    .await,
            );
        }
        WbFunction::Len(tx) => {
            let _ = tx.send(worterbuch.len());
        }
        WbFunction::ReGrantLocks(locks, tx) => {
            let _ = tx.send(worterbuch.re_grant_locks(locks).await);
        }
    }
    trace!(exit = "process_api_call");
}

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
