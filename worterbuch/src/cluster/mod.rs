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
    cluster::protocol::ClientWriteCommand,
    error::WorterbuchAppResult,
    server::common::WbFunction,
    worterbuch::{SubscriptionFlags, Worterbuch},
};
use serde::Serialize;
use tokio::sync::mpsc;
use tosub::SubsystemHandle;
use tracing::{Instrument, info};
use worterbuch_common::protocol::{ClientId, InternalAction, Trace, TraceData};

pub type ClusterStateChange = (ClientWriteCommand, ClientId, Trace);
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

async fn process_api_call(worterbuch: &mut Worterbuch, function: WbFunction) {
    match function {
        WbFunction::Get(key, tx) => {
            tx.send(worterbuch.get(&key)).ok();
        }
        WbFunction::CGet(key, tx) => {
            tx.send(worterbuch.cget(&key)).ok();
        }
        WbFunction::Set(transaction_id, interface, key, value, client_id, tx, span) => {
            tx.send(
                worterbuch
                    .set(
                        key,
                        value,
                        false,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .instrument(span)
                    .await,
            )
            .ok();
        }
        WbFunction::CSet(transaction_id, interface, key, value, version, client_id, tx) => {
            tx.send(
                worterbuch
                    .cset(
                        key,
                        value,
                        version,
                        false,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            )
            .ok();
        }
        WbFunction::SPubInit(transaction_id, interface, key, client_id, tx) => {
            tx.send(
                worterbuch
                    .spub_init(key, TraceData::new(client_id, interface, transaction_id))
                    .await,
            )
            .ok();
        }
        WbFunction::SPub(transaction_id, value, client_id, tx) => {
            tx.send(worterbuch.spub(transaction_id, value, client_id).await)
                .ok();
        }
        WbFunction::Publish(transaction_id, interface, key, value, client_id, tx) => {
            tx.send(
                worterbuch
                    .publish(
                        key,
                        value,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            )
            .ok();
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
            tx.send(
                worterbuch
                    .subscribe(
                        key,
                        SubscriptionFlags::new(unique, live_only, send_traces),
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            )
            .ok();
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
            tx.send(
                worterbuch
                    .psubscribe(
                        pattern,
                        SubscriptionFlags::new(unique, live_only, send_traces),
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            )
            .ok();
        }
        WbFunction::SubscribeLs(client_id, transaction_id, interface, parent, send_traces, tx) => {
            tx.send(
                worterbuch
                    .subscribe_ls(
                        parent,
                        send_traces,
                        TraceData::new(client_id, interface, transaction_id),
                    )
                    .await,
            )
            .ok();
        }
        WbFunction::Unsubscribe(client_id, transaction_id, interface, tx) => {
            tx.send(
                worterbuch
                    .unsubscribe(TraceData::new(client_id, interface, transaction_id))
                    .await,
            )
            .ok();
        }
        WbFunction::UnsubscribeLs(client_id, transaction_id, tx) => {
            tx.send(worterbuch.unsubscribe_ls(client_id, transaction_id))
                .ok();
        }
        WbFunction::Delete(transaction_id, interface, key, client_id, tx) => {
            tx.send(
                worterbuch
                    .delete(key, client_id, transaction_id, interface)
                    .await,
            )
            .ok();
        }
        WbFunction::PDelete(transaction_id, interface, pattern, client_id, tx) => {
            tx.send(
                worterbuch
                    .pdelete(pattern, client_id, transaction_id, interface)
                    .await,
            )
            .ok();
        }
        WbFunction::Lock(transaction_id, interface, key, client_id, tx) => {
            tx.send(
                worterbuch
                    .lock(key, client_id, transaction_id, interface)
                    .await,
            )
            .ok();
        }
        WbFunction::AcquireLock(transaction_id, interface, key, client_id, tx) => {
            tx.send(
                worterbuch
                    .acquire_lock(key, client_id, transaction_id, interface)
                    .await,
            )
            .ok();
        }
        WbFunction::ReleaseLock(transaction_id, interface, key, client_id, tx) => {
            tx.send(
                worterbuch
                    .release_lock(key, client_id, transaction_id, interface)
                    .await,
            )
            .ok();
        }
        WbFunction::Connected(client_id, remote_addr, protocol, tx) => {
            let res = worterbuch.connected(client_id, remote_addr, protocol).await;
            tx.send(res).ok();
        }
        WbFunction::ProtocolSwitched(client_id, interface, protocol) => {
            worterbuch
                .protocol_switched(client_id, interface, protocol)
                .await;
        }
        WbFunction::Disconnected(client_id, protocol, remote_addr) => {
            worterbuch
                .disconnected(client_id, protocol, remote_addr)
                .await
                .ok();
        }
        WbFunction::Config(tx) => {
            tx.send(worterbuch.config().clone()).ok();
        }
        WbFunction::Export(tx, span) => {
            let g = span.enter();
            worterbuch.export_for_persistence(tx);
            drop(g);
            drop(span);
        }
        WbFunction::Import(transaction_id, client_id, interface, json, tx) => {
            tx.send(
                worterbuch
                    .import(&json, client_id, transaction_id, interface)
                    .await,
            )
            .ok();
        }
        WbFunction::Len(tx) => {
            tx.send(worterbuch.len()).ok();
        }
    }
}

async fn shutdown(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    config: Config,
    servers: Servers,
) -> WorterbuchAppResult<()> {
    info!("Shutdown sequence triggered");

    subsys.request_global_shutdown();

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
        it.request_local_shutdown();
        it.join().await;
    }

    if let Some(it) = servers.tcp_server {
        info!("Shutting down tcp server …");
        it.request_local_shutdown();
        it.join().await;
    }

    if let Some(it) = servers.unix_socket {
        info!("Shutting down unix socket …");
        it.request_local_shutdown();
        it.join().await;
    }
}
