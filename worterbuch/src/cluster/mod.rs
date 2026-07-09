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
    Config, Servers, error::WorterbuchAppResult, server::common::WbFunction, worterbuch::Worterbuch,
};
use serde::Serialize;
use tosub::SubsystemHandle;
use tracing::{Instrument, info};

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
        WbFunction::Set(key, value, client_id, tx, span) => {
            tx.send(
                worterbuch
                    .set(key, value, client_id, false)
                    .instrument(span)
                    .await,
            )
            .ok();
        }
        WbFunction::CSet(key, value, version, client_id, tx) => {
            tx.send(worterbuch.cset(key, value, version, client_id, false).await)
                .ok();
        }
        WbFunction::SPubInit(transaction_id, key, client_id, tx) => {
            tx.send(worterbuch.spub_init(transaction_id, key, client_id).await)
                .ok();
        }
        WbFunction::SPub(transaction_id, value, client_id, tx) => {
            tx.send(worterbuch.spub(transaction_id, value, client_id).await)
                .ok();
        }
        WbFunction::Publish(key, value, tx) => {
            tx.send(worterbuch.publish(key, value).await).ok();
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
        WbFunction::Delete(key, client_id, tx) => {
            tx.send(worterbuch.delete(key, client_id).await).ok();
        }
        WbFunction::PDelete(pattern, client_id, tx) => {
            tx.send(worterbuch.pdelete(pattern, client_id).await).ok();
        }
        WbFunction::Lock(key, client_id, tx) => {
            tx.send(worterbuch.lock(key, client_id).await).ok();
        }
        WbFunction::AcquireLock(key, client_id, tx) => {
            tx.send(worterbuch.acquire_lock(key, client_id).await).ok();
        }
        WbFunction::ReleaseLock(key, client_id, tx) => {
            tx.send(worterbuch.release_lock(key, client_id).await).ok();
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
            let g = span.enter();
            worterbuch.export_for_persistence(tx);
            drop(g);
            drop(span);
        }
        WbFunction::Import(json, tx) => {
            tx.send(worterbuch.import(&json).await).ok();
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
        worterbuch.apply_all_grave_goods_and_last_wills().await;
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
