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

use crate::{
    cluster::{follower, leader, proxy, standalone},
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
use tokio::sync::{mpsc, oneshot};
use tosub::SubsystemHandle;
use tracing::{debug, info};
use worterbuch_common::{INTERNAL_CLIENT_ID, SYSTEM_TOPIC_NAME};

pub use config::*;
pub use worterbuch_common as common;

#[derive(Default)]
struct Servers {
    web_server: Option<SubsystemHandle>,
    tcp_server: Option<SubsystemHandle>,
    unix_socket: Option<SubsystemHandle>,
}

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

    wb_api_created(&api, tx);

    let mut worterbuch = persistence::restore(&subsys, config.clone(), api.clone()).await?;

    set_instance_name(&mut worterbuch, &config).await?;

    let web_server = web_server(&api, &subsys, &config);
    let tcp_server = tcp_server(&api, &subsys, &config);
    let unix_socket = unix_socket(&api, &subsys, &config);

    if config.role.provide_server_metadata() {
        server_metadata(api.clone(), &mut worterbuch, &subsys).await?;
    }

    match config.role.clone() {
        ClusterRole::Standalone => {
            standalone::run(
                &subsys,
                worterbuch,
                api_rx,
                config,
                Servers {
                    web_server,
                    tcp_server,
                    unix_socket,
                },
            )
            .await?;
        }
        ClusterRole::Leader { sync_port } => {
            leader::run(
                &subsys,
                worterbuch,
                api_rx,
                config,
                Servers {
                    web_server,
                    tcp_server,
                    unix_socket,
                },
                sync_port,
            )
            .await?;
        }
        ClusterRole::Follower { leader_address } => {
            follower::run(
                &subsys,
                worterbuch,
                api_rx,
                config,
                web_server,
                leader_address,
            )
            .await?;
        }
        ClusterRole::Proxy { leader_addresses } => {
            proxy::run(
                &subsys,
                worterbuch,
                api_rx,
                config,
                web_server,
                leader_addresses,
            )
            .await?;
        }
    }

    debug!("worterbuch subsystem completed.");

    Ok(())
}

fn wb_api_created(api: &CloneableWbApi, tx: Option<oneshot::Sender<CloneableWbApi>>) {
    if let Some(tx) = tx {
        tx.send(api.clone()).ok();
    }
}

async fn set_instance_name(
    worterbuch: &mut Worterbuch,
    config: &Config,
) -> Result<(), error::WorterbuchAppError> {
    if let Some(name) = config.instance_name.as_ref() {
        let key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_NAME);
        let value = json!(name);
        worterbuch.set(key, value, INTERNAL_CLIENT_ID, true).await?;
    }
    Ok(())
}

fn web_server(
    api: &CloneableWbApi,
    subsys: &SubsystemHandle,
    config: &Config,
) -> Option<SubsystemHandle> {
    if let Some(WsEndpoint {
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
        let ws_enabled = config.role.accept_client_connections() && !config.ws_disabled;
        Some(subsys.spawn("webserver", async move |subsys| {
            server::axum::start(sapi, tls, bind_addr, port, public_addr, subsys, ws_enabled).await
        }))
    } else {
        info!("Web server disabled.");
        None
    }
}

fn tcp_server(
    api: &CloneableWbApi,
    subsys: &SubsystemHandle,
    config: &Config,
) -> Option<SubsystemHandle> {
    let cfg = config.clone();
    if config.role.accept_client_connections()
        && let Some(Endpoint {
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
    }
}

fn unix_socket(
    api: &CloneableWbApi,
    subsys: &SubsystemHandle,
    config: &Config,
) -> Option<SubsystemHandle> {
    #[cfg(target_family = "unix")]
    if config.role.accept_client_connections()
        && let Some(UnixEndpoint { path }) = &config.unix_endpoint
        && !config.unix_disabled
    {
        let sapi = api.clone();
        let path = path.clone();
        Some(subsys.spawn("unixsocket", async move |subsys| {
            server::unix::start(sapi, path, subsys).await
        }))
    } else {
        None
    }

    #[cfg(not(target_family = "unix"))]
    None
}

async fn server_metadata(
    api: CloneableWbApi,
    worterbuch: &mut Worterbuch,
    subsys: &SubsystemHandle,
) -> Result<(), error::WorterbuchAppError> {
    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION),
            serde_json::to_value(SUPPORTED_PROTOCOL_VERSIONS)
                .unwrap_or_else(|e| Value::String(format!("Error serializing version: {e}"))),
            INTERNAL_CLIENT_ID,
            true,
        )
        .await?;

    subsys.spawn("stats", async |subsys| track_stats(api, subsys).await);

    Ok(())
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
