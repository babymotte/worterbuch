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
mod cluster;
mod config;
pub mod error;
pub(crate) mod license;
#[cfg(not(feature = "telemetry"))]
pub mod logging;
#[cfg(not(feature = "jemalloc"))]
mod mem_tools;
mod persistence;
pub mod server;
mod stats;
pub(crate) mod store;
mod subscribers;
#[cfg(feature = "telemetry")]
pub mod telemetry;
mod worterbuch;

pub use config::*;
use tosub::SubsystemHandle;
pub use worterbuch_common as common;

use crate::{
    cluster::{follower::run_in_follower_mode, leader::run_in_leader_mode},
    error::WorterbuchAppResult,
    server::{CloneableWbApi, common::SUPPORTED_PROTOCOL_VERSIONS},
    stats::track_stats,
    worterbuch::Worterbuch,
};
use cluster::ClientWriteCommand;
use common::{
    SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_ROOT_PREFIX, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION, Value,
    topic,
};
use serde_json::json;
use server::common::WbFunction;
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tracing::{Instrument, debug, info};
use worterbuch_common::{INTERNAL_CLIENT_ID, SYSTEM_TOPIC_NAME};

pub async fn spawn_worterbuch(
    subsys: &SubsystemHandle,
    config: Config,
) -> WorterbuchAppResult<CloneableWbApi> {
    let (api_tx, api_rx) = oneshot::channel();
    subsys.spawn("worterbuch", |s| do_run_worterbuch(s, config, Some(api_tx)));
    Ok(api_rx.await?)
}

pub async fn run_worterbuch(subsys: SubsystemHandle, config: Config) -> WorterbuchAppResult<()> {
    do_run_worterbuch(subsys, config, None).await?;
    Ok(())
}

async fn do_run_worterbuch(
    subsys: SubsystemHandle,
    config: Config,
    tx: Option<oneshot::Sender<CloneableWbApi>>,
) -> WorterbuchAppResult<()> {
    let channel_buffer_size = config.channel_buffer_size;
    let (api_tx, api_rx) = mpsc::channel(channel_buffer_size);
    let api = CloneableWbApi::new(api_tx, config.clone());

    if let Some(tx) = tx {
        tx.send(api.clone()).ok();
    }

    let mut worterbuch = persistence::restore(&subsys, &config, &api).await?;

    if let Some(name) = config.instance_name.as_ref() {
        worterbuch
            .set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_NAME),
                json!(name),
                INTERNAL_CLIENT_ID,
                true,
            )
            .await?;
    }

    let web_server = if let Some(WsEndpoint {
        endpoint: Endpoint {
            tls,
            bind_addr,
            port,
        },
        public_addr,
    }) = &config.ws_endpoint
    {
        info!("Starting web server …");
        let sapi = api.clone();
        let tls = tls.to_owned();
        let bind_addr = bind_addr.to_owned();
        let port = port.to_owned();
        let public_addr = public_addr.to_owned();
        let ws_enabled = !config.ws_disabled && !config.follower;
        Some(subsys.spawn("webserver", async move |subsys| {
            server::axum::start(sapi, tls, bind_addr, port, public_addr, subsys, ws_enabled).await
        }))
    } else {
        info!("Web server disabled.");
        None
    };

    if config.follower {
        run_in_follower_mode(&subsys, worterbuch, api_rx, config, web_server).await?;
    } else {
        worterbuch
            .set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION),
                serde_json::to_value(SUPPORTED_PROTOCOL_VERSIONS)
                    .unwrap_or_else(|e| Value::String(format!("Error serializing version: {e}"))),
                INTERNAL_CLIENT_ID,
                true,
            )
            .await?;

        let worterbuch_uptime = api.clone();
        subsys.spawn("stats", async |subsys| {
            track_stats(worterbuch_uptime, subsys).await
        });

        let cfg = config.clone();
        let tcp_server = if let Some(Endpoint {
            tls: _,
            bind_addr,
            port,
        }) = &config.tcp_endpoint
            && !config.tcp_disabled
        {
            let sapi = api.clone();
            let bind_addr = bind_addr.to_owned();
            let port = port.to_owned();
            Some(subsys.spawn("tcpserver", async move |subsys| {
                server::tcp::start(sapi, cfg, bind_addr, port, subsys).await
            }))
        } else {
            None
        };

        #[cfg(target_family = "unix")]
        let unix_socket = if let Some(UnixEndpoint { path }) = &config.unix_endpoint
            && !config.unix_disabled
        {
            let sapi = api.clone();
            let path = path.clone();
            Some(subsys.spawn("unixsocket", async move |subsys| {
                server::unix::start(sapi, path, subsys).await
            }))
        } else {
            None
        };

        #[cfg(not(target_family = "unix"))]
        let unix_socket = None;

        if config.leader {
            run_in_leader_mode(
                &subsys,
                worterbuch,
                api_rx,
                config,
                web_server,
                tcp_server,
                unix_socket,
            )
            .await?;
        } else {
            run_in_regular_mode(
                &subsys,
                worterbuch,
                api_rx,
                config,
                web_server,
                tcp_server,
                unix_socket,
            )
            .await?;
        }
    }

    debug!("worterbuch subsystem completed.");

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

async fn run_in_regular_mode(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    web_server: Option<SubsystemHandle>,
    tcp_server: Option<SubsystemHandle>,
    unix_socket: Option<SubsystemHandle>,
) -> WorterbuchAppResult<()> {
    loop {
        select! {
            recv = api_rx.recv() => match recv {
                Some(function) => process_api_call(&mut worterbuch, function).await,
                None => break,
            },
            _ = subsys.shutdown_requested() => break,
        }
    }

    shutdown(
        subsys,
        worterbuch,
        config,
        web_server,
        tcp_server,
        unix_socket,
    )
    .await
}

async fn shutdown(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    config: Config,
    web_server: Option<SubsystemHandle>,
    tcp_server: Option<SubsystemHandle>,
    unix_socket: Option<SubsystemHandle>,
) -> WorterbuchAppResult<()> {
    info!("Shutdown sequence triggered");

    subsys.request_global_shutdown();

    shutdown_servers(web_server, tcp_server, unix_socket).await;

    if config.use_persistence {
        info!("Applying grave goods and last wills …");
        worterbuch.apply_all_grave_goods_and_last_wills().await;
        info!("Waiting for persistence hook to complete …");
        worterbuch.flush().await?;
        info!("Shutdown persistence hook complete.");
    }

    Ok(())
}

async fn shutdown_servers(
    web_server: Option<SubsystemHandle>,
    tcp_server: Option<SubsystemHandle>,
    unix_socket: Option<SubsystemHandle>,
) {
    if let Some(it) = web_server {
        info!("Shutting down web server …");
        it.request_local_shutdown();
        it.join().await;
    }

    if let Some(it) = tcp_server {
        info!("Shutting down tcp server …");
        it.request_local_shutdown();
        it.join().await;
    }

    if let Some(it) = unix_socket {
        info!("Shutting down unix socket …");
        it.request_local_shutdown();
        it.join().await;
    }
}

async fn forward_api_call(
    client_write_txs: &mut Vec<(usize, mpsc::Sender<ClientWriteCommand>)>,
    dead: &mut Vec<usize>,
    function: &WbFunction,
    filter_sys: bool,
) {
    if let Some(cmd) = match function {
        WbFunction::Get(_, _)
        | WbFunction::CGet(_, _)
        | WbFunction::SPubInit(_, _, _, _)
        | WbFunction::SPub(_, _, _, _)
        | WbFunction::Publish(_, _, _)
        | WbFunction::Ls(_, _)
        | WbFunction::PLs(_, _)
        | WbFunction::PGet(_, _)
        | WbFunction::Subscribe(_, _, _, _, _, _)
        | WbFunction::PSubscribe(_, _, _, _, _, _)
        | WbFunction::SubscribeLs(_, _, _, _)
        | WbFunction::Unsubscribe(_, _, _)
        | WbFunction::UnsubscribeLs(_, _, _)
        | WbFunction::Connected(_, _, _, _)
        | WbFunction::ProtocolSwitched(_, _)
        | WbFunction::Disconnected(_, _)
        | WbFunction::Config(_)
        | WbFunction::Export(_, _)
        | WbFunction::Import(_, _)
        | WbFunction::Len(_)
        | WbFunction::Lock(_, _, _)
        | WbFunction::AcquireLock(_, _, _)
        | WbFunction::ReleaseLock(_, _, _) => None,
        WbFunction::Set(key, value, _, _, _) => {
            if !filter_sys || !key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX) {
                Some(ClientWriteCommand::Set(
                    key.to_owned(),
                    value.to_owned(),
                    false,
                ))
            } else {
                None
            }
        }
        WbFunction::CSet(key, value, version, _, _) => {
            if !filter_sys || !key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX) {
                Some(ClientWriteCommand::CSet(
                    key.to_owned(),
                    value.to_owned(),
                    version.to_owned(),
                    false,
                ))
            } else {
                None
            }
        }
        WbFunction::Delete(key, _, _) => {
            if !filter_sys || !key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX) {
                Some(ClientWriteCommand::Delete(key.to_owned()))
            } else {
                None
            }
        }
        WbFunction::PDelete(pattern, _, _) => {
            if !filter_sys || !pattern.starts_with(SYSTEM_TOPIC_ROOT_PREFIX) {
                Some(ClientWriteCommand::PDelete(pattern.to_owned()))
            } else {
                None
            }
        }
    } {
        forward_to_followers(cmd, client_write_txs, dead).await;
    }
}

async fn forward_to_followers(
    cmd: ClientWriteCommand,
    client_write_txs: &mut Vec<(usize, mpsc::Sender<ClientWriteCommand>)>,
    dead: &mut Vec<usize>,
) {
    for (id, tx) in client_write_txs.iter() {
        if tx.send(cmd.clone()).await.is_err() {
            dead.push(*id);
        }
    }
    if !dead.is_empty() {
        client_write_txs.retain(|(i, _)| !dead.contains(i));
        dead.clear();
    }
}
