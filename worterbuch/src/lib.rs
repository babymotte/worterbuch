/*
 *  The worterbuch application library
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

//! This library allows you to embed worterbuch into your application.
//!
//! Note that while it makes embedding very easy, it does leak several
//! dependencies into your application that a proper library normally
//! shouldn't. Worterbuch takes this liberty because it is essentailly
//! still an application. Just one that you can start from within your
//! own application.

mod auth;
mod config;
pub mod license;
mod persistence;
mod server;
mod stats;
pub mod store;
mod subscribers;
mod worterbuch;

use crate::stats::track_stats;
pub use crate::worterbuch::*;
use anyhow::Result;
pub use config::*;
use serde_json::Value;
use server::common::{CloneableWbApi, WbFunction};
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot},
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use uuid::Uuid;
use worterbuch_common::{topic, SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION};

pub const INTERNAL_CLIENT_ID: Uuid = Uuid::nil();

pub async fn run_worterbuch(subsys: SubsystemHandle, config: Config) -> Result<()> {
    let config_pers = config.clone();

    let channel_buffer_size = config.channel_buffer_size;

    let use_persistence = config.use_persistence;

    let mut worterbuch = if use_persistence {
        persistence::load(config.clone()).await?
    } else {
        Worterbuch::with_config(config.clone())
    };

    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION),
            serde_json::to_value(worterbuch.supported_protocol_version())
                .unwrap_or_else(|e| Value::String(format!("Error serializing version: {e}"))),
            INTERNAL_CLIENT_ID,
        )
        .await?;

    let (api_tx, mut api_rx) = mpsc::channel(channel_buffer_size);
    let api = CloneableWbApi::new(api_tx);

    let worterbuch_pers = api.clone();
    let worterbuch_uptime = api.clone();

    if use_persistence {
        subsys.start(SubsystemBuilder::new("persistence", |subsys| {
            persistence::periodic(worterbuch_pers, config_pers, subsys)
        }));
    }

    subsys.start(SubsystemBuilder::new("stats", |subsys| {
        track_stats(worterbuch_uptime, subsys)
    }));

    if let Some(WsEndpoint {
        endpoint: Endpoint {
            tls,
            bind_addr,
            port,
        },
        public_addr,
    }) = &config.ws_endpoint
    {
        let sapi = api.clone();
        let tls = tls.to_owned();
        let bind_addr = bind_addr.to_owned();
        let port = port.to_owned();
        let public_addr = public_addr.to_owned();
        subsys.start(SubsystemBuilder::new("webserver", move |subsys| {
            server::poem::start(sapi, tls, bind_addr, port, public_addr, subsys)
        }));
    }

    if let Some(Endpoint {
        tls: _,
        bind_addr,
        port,
    }) = &config.tcp_endpoint
    {
        let sapi = api.clone();
        let bind_addr = bind_addr.to_owned();
        let port = port.to_owned();
        subsys.start(SubsystemBuilder::new("tcpserver", move |subsys| {
            server::tcp::start(sapi, bind_addr, port, subsys)
        }));
    }

    #[cfg(target_family = "unix")]
    if let Some(UnixEndpoint { path }) = &config.unix_endpoint {
        let sapi = api.clone();
        let path = path.clone();
        subsys.start(SubsystemBuilder::new("unixserver", move |subsys| {
            server::unix::start(sapi, path, subsys)
        }));
    }

    let (persist_tx, mut persist_rx) = oneshot::channel();
    let mut persist_tx = Some(persist_tx);

    loop {
        select! {
            recv = api_rx.recv() => match recv {
                Some(function) => process_api_call(&mut worterbuch, function).await,
                None => break,
            },
            () = subsys.on_shutdown_requested() => {
                if use_persistence {
                    if let Some(persist_tx) = persist_tx.take() {
                        let api = api.clone();
                        let config = config.clone();
                        spawn(async move {
                            if let Err(e) = persistence::once(&api, &config).await {
                                log::warn!("Could not persist state: {e}");
                            }
                            persist_tx.send(()).ok();
                        });
                    }
                }
                break;
            },
        }
    }

    if use_persistence {
        log::info!("Shutdown sequence triggered, waiting for persistence hook to complete …");

        loop {
            select! {
                recv = api_rx.recv() => match recv {
                    Some(function) => process_api_call(&mut worterbuch, function).await,
                    None => break,
                },
                _ = &mut persist_rx => {
                    log::info!("Shutdown persistence hook complete.");
                    break;
                },
            }
        }
    }

    log::debug!("worterbuch subsystem completed.");

    Ok(())
}

async fn process_api_call(worterbuch: &mut Worterbuch, function: WbFunction) {
    match function {
        WbFunction::Get(key, tx) => {
            tx.send(worterbuch.get(&key)).ok();
        }
        WbFunction::Set(key, value, client_id, tx) => {
            tx.send(worterbuch.set(key, value, client_id).await).ok();
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
        WbFunction::Connected(client_id, remote_addr, protocol) => {
            worterbuch
                .connected(client_id, remote_addr, &protocol)
                .await;
        }
        WbFunction::Disconnected(client_id, remote_addr) => {
            worterbuch.disconnected(client_id, remote_addr).await.ok();
        }
        WbFunction::Config(tx) => {
            tx.send(worterbuch.config().clone()).ok();
        }
        WbFunction::Export(tx) => {
            worterbuch.export(tx);
        }
        WbFunction::Len(tx) => {
            tx.send(worterbuch.len()).ok();
        }
        WbFunction::SupportedProtocolVersion(tx) => {
            tx.send(worterbuch.supported_protocol_version()).ok();
        }
    }
}
