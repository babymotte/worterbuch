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

use core::fmt;

use crate::{
    cluster::{follower, leader, proxy, standalone},
    error::WorterbuchAppResult,
    server::common::CloneableWbApi,
    stats::track_stats,
    worterbuch::Worterbuch,
};
use serde_json::json;
use tokio::sync::{mpsc, oneshot};
use tosub::Subsystem;
use tracing::{debug, info};
use worterbuch_common::{
    INTERNAL_CLIENT_ID, Protocol, WorterbuchVersion,
    protocol::v1::{
        Interface, InternalAction, SYSTEM_TOPIC_NAME, SYSTEM_TOPIC_ROOT,
        SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION, Trace, Value,
    },
    topic,
};

pub use config::*;
pub use worterbuch_common as common;

const WORTERBUCH_VERSION: (&str, &str, &str) = (
    env!("CARGO_PKG_VERSION_MAJOR"),
    env!("CARGO_PKG_VERSION_MINOR"),
    env!("CARGO_PKG_VERSION_PATCH"),
);

pub fn worterbuch_version() -> WorterbuchVersion {
    WorterbuchVersion(
        WORTERBUCH_VERSION
            .0
            .parse::<u32>()
            .expect("invalid cargo version"),
        WORTERBUCH_VERSION
            .1
            .parse::<u32>()
            .expect("invalid cargo version"),
        WORTERBUCH_VERSION
            .2
            .parse::<u32>()
            .expect("invalid cargo version"),
    )
}

#[derive(Default)]
struct Servers {
    web_server: Option<Subsystem>,
    tcp_server: Option<Subsystem>,
    unix_socket: Option<Subsystem>,
    quic_server: Option<Subsystem>,
}

pub async fn spawn_worterbuch(
    subsys: &Subsystem,
    config: Config,
    stdin: Option<mpsc::Receiver<String>>,
) -> WorterbuchAppResult<CloneableWbApi> {
    let (api_tx, api_rx) = oneshot::channel();
    subsys.spawn("worterbuch", |s| {
        do_run_worterbuch(s, config, Some(api_tx), stdin)
    });
    Ok(api_rx.await?)
}

pub async fn run_worterbuch(
    subsys: Subsystem,
    config: Config,
    stdin: Option<mpsc::Receiver<String>>,
) -> WorterbuchAppResult<()> {
    do_run_worterbuch(subsys, config, None, stdin).await?;
    Ok(())
}

async fn do_run_worterbuch(
    subsys: Subsystem,
    config: Config,
    tx: Option<oneshot::Sender<CloneableWbApi>>,
    stdin: Option<mpsc::Receiver<String>>,
) -> WorterbuchAppResult<()> {
    let channel_buffer_size = config.channel_buffer_size;
    let (api_tx, api_rx) = mpsc::channel(channel_buffer_size);
    let supported_client_protocol_versions = config.supported_client_protocol_versions();
    let api = CloneableWbApi::new(
        api_tx,
        config.clone(),
        Interface::Local,
        &supported_client_protocol_versions,
    );

    wb_api_created(&api, tx, "client/internal");

    let mut worterbuch =
        persistence::restore(&subsys, config.clone(), api.named("worterbuch-core")).await?;

    set_instance_name(&mut worterbuch, &config).await?;

    let web_server = web_server(&api, &subsys, &config);
    let tcp_server = tcp_server(&api, &subsys, &config);
    let unix_socket = unix_socket(&api, &subsys, &config);
    let quic_server = quic_server(&api, &subsys, &config);

    if config.role.provide_server_metadata() {
        server_metadata(api.named("server-metadata"), &mut worterbuch, &subsys).await?;
    }

    let stdin = stdin.unwrap_or_else(|| mpsc::channel(1).1);

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
                    quic_server,
                },
            )
            .await?;
        }
        ClusterRole::Leader { sync_port } => {
            leader::run(
                &subsys,
                worterbuch,
                &api,
                api_rx,
                config,
                Servers {
                    web_server,
                    tcp_server,
                    unix_socket,
                    quic_server,
                },
                sync_port,
            )
            .await?;
        }
        ClusterRole::Follower { leader_address } => {
            follower::run(&subsys, worterbuch, config, web_server, leader_address).await?;
        }
        ClusterRole::Proxy { leader_addresses } => {
            proxy::run(
                &subsys,
                worterbuch,
                api_rx,
                config,
                Servers {
                    web_server,
                    tcp_server,
                    unix_socket,
                    quic_server,
                },
                &leader_addresses,
                stdin,
            )
            .await?;
        }
    }

    debug!("worterbuch subsystem completed.");

    Ok(())
}

fn wb_api_created(
    api: &CloneableWbApi,
    tx: Option<oneshot::Sender<CloneableWbApi>>,
    name: impl fmt::Display,
) {
    if let Some(tx) = tx {
        tx.send(api.named(name)).ok();
    }
}

async fn set_instance_name(
    worterbuch: &mut Worterbuch,
    config: &Config,
) -> Result<(), error::WorterbuchAppError> {
    if let Some(name) = config.instance_name.as_ref() {
        let key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_NAME);
        let value = json!(name);
        worterbuch
            .internal_set(
                key,
                value,
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::Startup),
                true,
            )
            .await?;
    }
    Ok(())
}

fn web_server(api: &CloneableWbApi, subsys: &Subsystem, config: &Config) -> Option<Subsystem> {
    if let Some(Endpoint {
        tls,
        bind_addr,
        port,
    }) = &config.ws_endpoint
    {
        info!("Starting web server …");
        let sapi = api.for_interface("server/http", Interface::Protocol(Protocol::HTTP));
        let tls = tls.to_owned();
        let bind_addr = bind_addr.to_owned();
        let port = port.to_owned();
        let ws_enabled = config.role.accept_client_connections() && !config.ws_disabled;
        Some(subsys.spawn("webserver", async move |subsys| {
            server::axum::start(sapi, tls, bind_addr, port, subsys, ws_enabled).await
        }))
    } else {
        info!("Web server disabled.");
        None
    }
}

fn tcp_server(api: &CloneableWbApi, subsys: &Subsystem, config: &Config) -> Option<Subsystem> {
    let cfg = config.clone();
    if config.role.accept_client_connections()
        && let Some(Endpoint {
            tls: _,
            bind_addr,
            port,
        }) = &config.tcp_endpoint
        && !config.tcp_disabled
    {
        let sapi = api.for_interface("server/tcp", Interface::Protocol(Protocol::TCP));
        let bind_addr = bind_addr.to_owned();
        let port = port.to_owned();
        Some(subsys.spawn("tcpserver", async move |subsys| {
            server::tcp::start(sapi, cfg, bind_addr, port, subsys).await
        }))
    } else {
        None
    }
}

fn unix_socket(api: &CloneableWbApi, subsys: &Subsystem, config: &Config) -> Option<Subsystem> {
    #[cfg(target_family = "unix")]
    if config.role.accept_client_connections()
        && let Some(UnixEndpoint { path }) = &config.unix_endpoint
        && !config.unix_disabled
    {
        let sapi = api.for_interface("server/unix", Interface::Protocol(Protocol::UNIX));
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

fn quic_server(api: &CloneableWbApi, subsys: &Subsystem, config: &Config) -> Option<Subsystem> {
    if config.role.accept_client_connections()
        && let Some(QuicEndpoint {
            bind_addr,
            port,
            cert_path,
            key_path,
        }) = &config.quic_endpoint
        && !config.quic_disabled
    {
        let sapi = api.for_interface("server/quic", Interface::Protocol(Protocol::QUIC));
        let bind_addr = bind_addr.to_owned();
        let port = port.to_owned();
        let cert_path = cert_path.clone();
        let key_path = key_path.clone();
        Some(subsys.spawn("quicserver", async move |subsys| {
            server::quic::start(sapi, bind_addr, port, cert_path, key_path, subsys).await
        }))
    } else {
        None
    }
}

async fn server_metadata(
    api: CloneableWbApi,
    worterbuch: &mut Worterbuch,
    subsys: &Subsystem,
) -> Result<(), error::WorterbuchAppError> {
    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION),
            serde_json::to_value(worterbuch.config().supported_client_protocol_versions())
                .unwrap_or_else(|e| Value::String(format!("Error serializing version: {e}"))),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::Startup),
            true,
        )
        .await?;

    subsys.spawn("stats", async |subsys| track_stats(api, subsys).await);

    Ok(())
}
