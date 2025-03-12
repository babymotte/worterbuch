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
mod leader_follower;
pub mod license;
mod persistence;
mod server;
mod stats;
pub mod store;
mod subscribers;
mod worterbuch;

use crate::stats::track_stats;
pub use crate::worterbuch::*;
pub use config::*;
use leader_follower::{
    ClientWriteCommand, Mode, StateSync, process_leader_message, run_cluster_sync_port,
    shutdown_on_stdin_close,
};
use miette::{IntoDiagnostic, Result, miette};
use serde_json::{Value, json};
use server::common::{CloneableWbApi, WbFunction};
use std::{
    error::Error,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    select,
    sync::{mpsc, oneshot},
    time::interval,
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use worterbuch_common::{
    KeySegment, PStateEvent, ProtocolVersion, SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_GRAVE_GOODS,
    SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_ROOT_PREFIX,
    SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION, error::WorterbuchError, tcp::receive_msg, topic,
};

pub const SUPPORTED_PROTOCOL_VERSIONS: [ProtocolVersion; 2] =
    [ProtocolVersion::new(0, 11), ProtocolVersion::new(1, 1)];

pub const INTERNAL_CLIENT_ID: Uuid = Uuid::nil();

type ServerSubsystem = tokio_graceful_shutdown::NestedSubsystem<Box<dyn Error + Send + Sync>>;

pub async fn run_worterbuch(subsys: SubsystemHandle, config: Config) -> Result<()> {
    let config_pers = config.clone();

    let channel_buffer_size = config.channel_buffer_size;

    let use_persistence = config.use_persistence;

    let mut worterbuch = if use_persistence && !config.follower {
        persistence::load(config.clone()).await?
    } else {
        Worterbuch::with_config(config.clone())
    };

    let (api_tx, api_rx) = mpsc::channel(channel_buffer_size);
    let api = CloneableWbApi::new(api_tx);

    let web_server = if let Some(WsEndpoint {
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
        let ws_enabled = !config.follower;
        Some(
            subsys.start(SubsystemBuilder::new("webserver", move |subsys| {
                server::poem::start(sapi, tls, bind_addr, port, public_addr, subsys, ws_enabled)
            })),
        )
    } else {
        None
    };

    if config.follower {
        run_in_follower_mode(subsys, &mut worterbuch, api_rx, config, web_server).await?;
    } else {
        worterbuch
            .set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION),
                serde_json::to_value(SUPPORTED_PROTOCOL_VERSIONS)
                    .unwrap_or_else(|e| Value::String(format!("Error serializing version: {e}"))),
                INTERNAL_CLIENT_ID,
            )
            .await?;

        if use_persistence {
            let worterbuch_pers = api.clone();
            subsys.start(SubsystemBuilder::new("persistence", |subsys| {
                persistence::periodic(worterbuch_pers, config_pers, subsys)
            }));
        }

        let worterbuch_uptime = api.clone();
        subsys.start(SubsystemBuilder::new("stats", |subsys| {
            track_stats(worterbuch_uptime, subsys)
        }));

        let tcp_server = if let Some(Endpoint {
            tls: _,
            bind_addr,
            port,
        }) = &config.tcp_endpoint
        {
            let sapi = api.clone();
            let bind_addr = bind_addr.to_owned();
            let port = port.to_owned();
            Some(
                subsys.start(SubsystemBuilder::new("tcpserver", move |subsys| {
                    server::tcp::start(sapi, bind_addr, port, subsys)
                })),
            )
        } else {
            None
        };

        #[cfg(target_family = "unix")]
        let unix_socket = if let Some(UnixEndpoint { path }) = &config.unix_endpoint {
            let sapi = api.clone();
            let path = path.clone();
            Some(
                subsys.start(SubsystemBuilder::new("unixsocket", move |subsys| {
                    server::unix::start(sapi, path, subsys)
                })),
            )
        } else {
            None
        };

        #[cfg(not(target_family = "unix"))]
        let unix_socket = None;

        if config.leader {
            run_in_leader_mode(
                subsys,
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
                subsys,
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
        WbFunction::Set(key, value, client_id, tx) => {
            tx.send(worterbuch.set(key, value, client_id).await).ok();
        }
        WbFunction::CSet(key, value, version, client_id, tx) => {
            tx.send(worterbuch.cset(key, value, version, client_id).await)
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
            tx.send(worterbuch.lock(key, client_id)).ok();
        }
        WbFunction::ReleaseLock(key, client_id, tx) => {
            tx.send(worterbuch.release_lock(key, client_id)).ok();
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
            worterbuch.export_for_persistence(tx);
        }
        WbFunction::Len(tx) => {
            tx.send(worterbuch.len()).ok();
        }
    }
}

async fn process_api_call_as_follower(worterbuch: &mut Worterbuch, function: WbFunction) {
    match function {
        WbFunction::Get(key, tx) => {
            tx.send(worterbuch.get(&key)).ok();
        }
        WbFunction::CGet(key, tx) => {
            tx.send(worterbuch.cget(&key)).ok();
        }
        WbFunction::Set(_, _, _, tx) => {
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
        WbFunction::Lock(key, client_id, tx) => {
            tx.send(worterbuch.lock(key, client_id)).ok();
        }
        WbFunction::ReleaseLock(key, client_id, tx) => {
            tx.send(worterbuch.release_lock(key, client_id)).ok();
        }
        WbFunction::Delete(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
        }
        WbFunction::PDelete(_, _, tx) => {
            tx.send(Err(WorterbuchError::NotLeader)).ok();
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
            worterbuch.export_for_persistence(tx);
        }
        WbFunction::Len(tx) => {
            tx.send(worterbuch.len()).ok();
        }
    }
}

async fn run_in_regular_mode(
    subsys: SubsystemHandle,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    web_server: Option<ServerSubsystem>,
    tcp_server: Option<ServerSubsystem>,
    unix_socket: Option<ServerSubsystem>,
) -> Result<()> {
    loop {
        select! {
            recv = api_rx.recv() => match recv {
                Some(function) => process_api_call(&mut worterbuch, function).await,
                None => break,
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    shutdown(
        subsys,
        &mut worterbuch,
        config,
        web_server,
        tcp_server,
        unix_socket,
    )
    .await
}

async fn run_in_leader_mode(
    subsys: SubsystemHandle,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    web_server: Option<ServerSubsystem>,
    tcp_server: Option<ServerSubsystem>,
    unix_socket: Option<ServerSubsystem>,
) -> Result<()> {
    info!("Running in LEADER mode.");

    shutdown_on_stdin_close(&subsys);

    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Leader),
            INTERNAL_CLIENT_ID,
        )
        .await?;

    let mut client_write_txs: Vec<(usize, mpsc::Sender<ClientWriteCommand>)> = vec![];
    let (follower_connected_tx, mut follower_connected_rx) = mpsc::channel::<
        oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    >(config.channel_buffer_size);

    let mut tx_id = 0;
    let mut dead = vec![];

    let cfg = config.clone();
    subsys.start(SubsystemBuilder::new("cluster_sync_port", move |s| {
        run_cluster_sync_port(s, cfg, follower_connected_tx)
    }));

    let (mut grave_goods_rx, _) = worterbuch
        .psubscribe(
            INTERNAL_CLIENT_ID,
            0,
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                KeySegment::Wildcard,
                SYSTEM_TOPIC_GRAVE_GOODS
            ),
            true,
            false,
        )
        .await?;
    let (mut last_will_rx, _) = worterbuch
        .psubscribe(
            INTERNAL_CLIENT_ID,
            0,
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                KeySegment::Wildcard,
                SYSTEM_TOPIC_LAST_WILL
            ),
            true,
            false,
        )
        .await?;

    loop {
        select! {
            recv = grave_goods_rx.recv() => if let Some(e) = recv {
                debug!("Forwarding grave goods change: {e:?}");
                match e {
                    PStateEvent::KeyValuePairs(kvps) => {
                        for kvp in kvps {
                            forward_api_call(&mut client_write_txs, &mut dead, &WbFunction::Set(kvp.key, kvp.value, INTERNAL_CLIENT_ID, oneshot::channel().0), false).await;
                        }
                    },
                    PStateEvent::Deleted(kvps) => {
                        for kvp in kvps {
                            forward_api_call(&mut client_write_txs, &mut dead, &WbFunction::Delete(kvp.key, INTERNAL_CLIENT_ID, oneshot::channel().0), false).await;
                        }
                    },
                }
            },
            recv = last_will_rx.recv() => if let Some(e) = recv {
                debug!("Forwarding last will change: {e:?}");
                match e {
                    PStateEvent::KeyValuePairs(kvps) => {
                        for kvp in kvps {
                            forward_api_call(&mut client_write_txs, &mut dead, &WbFunction::Set(kvp.key, kvp.value, INTERNAL_CLIENT_ID, oneshot::channel().0), false).await;
                        }
                    },
                    PStateEvent::Deleted(kvps) => {
                        for kvp in kvps {
                            forward_api_call(&mut client_write_txs, &mut dead, &WbFunction::Delete(kvp.key, INTERNAL_CLIENT_ID, oneshot::channel().0), false).await;
                        }
                    },
                }
            },
            recv = api_rx.recv() => match recv {
                Some(function) => {
                    forward_api_call(&mut client_write_txs, &mut dead, &function, true).await;
                    process_api_call(&mut worterbuch, function).await;
                },
                None => break,
            },
            recv = follower_connected_rx.recv() => match recv {
                Some(state_tx) => {
                    let (client_write_tx, client_write_rx) = mpsc::channel(config.channel_buffer_size);
                    let (current_state, grave_goods, last_will) = worterbuch.export();
                    if state_tx.send((StateSync(current_state, grave_goods, last_will), client_write_rx)).is_ok() {
                        client_write_txs.push((tx_id, client_write_tx));
                        tx_id += 1;
                    }
                },
                None => break,
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    shutdown(
        subsys,
        &mut worterbuch,
        config,
        web_server,
        tcp_server,
        unix_socket,
    )
    .await
}

async fn run_in_follower_mode(
    subsys: SubsystemHandle,
    worterbuch: &mut Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    web_server: Option<ServerSubsystem>,
) -> Result<()> {
    let leader_addr = if let Some(it) = &config.leader_address {
        it
    } else {
        return Err(miette!("No valid leader address configured."));
    };
    info!("Running in FOLLOWER mode. Leader: {}", leader_addr,);

    shutdown_on_stdin_close(&subsys);

    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Follower),
            INTERNAL_CLIENT_ID,
        )
        .await?;

    // TODO configure lower persistence interval for follower
    let persistence_interval = config.persistence_interval;
    let mut last_persisted: Option<Instant> = None;
    let mut interval = interval(config.persistence_interval);
    let mut initial_sync_complete = false;

    let stream = TcpStream::connect(leader_addr).await.into_diagnostic()?;

    let mut lines = BufReader::new(stream).lines();

    loop {
        select! {
            recv = receive_msg(&mut lines) => match recv {
                Ok(Some(msg)) => {
                    initial_sync_complete |= process_leader_message(msg, worterbuch).await?;

                    persist(worterbuch, &config, initial_sync_complete,  persistence_interval, &mut last_persisted).await;
                },
                Ok(None) => break,
                Err(e) => {
                    error!("Error receiving update from leader: {e}");
                    break;
                }
            },
            recv = api_rx.recv() => match recv {
                Some(function) => process_api_call_as_follower(worterbuch, function).await,
                None => break,
            },
            _ = interval.tick() => persist(worterbuch, &config, initial_sync_complete,  persistence_interval, &mut last_persisted).await,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    shutdown(subsys, worterbuch, config, web_server, None, None).await
}

async fn persist(
    worterbuch: &mut Worterbuch,
    config: &Config,
    initial_sync_complete: bool,
    persistence_interval: Duration,
    last_persisted: &mut Option<Instant>,
) {
    let persist = initial_sync_complete
        && match last_persisted {
            Some(last) => last.elapsed() >= persistence_interval,
            None => true,
        };

    if persist {
        if let Err(e) = persistence::synchronous(worterbuch, config).await {
            warn!("Could not persist state: {e}");
        } else {
            *last_persisted = Some(Instant::now());
        }
    }
}

async fn shutdown(
    subsys: SubsystemHandle,
    worterbuch: &mut Worterbuch,
    config: Config,
    web_server: Option<ServerSubsystem>,
    tcp_server: Option<ServerSubsystem>,
    unix_socket: Option<ServerSubsystem>,
) -> Result<()> {
    info!("Shutdown sequence triggered");

    subsys.request_shutdown();

    shutdown_servers(web_server, tcp_server, unix_socket).await;

    info!("Applying grave goods and last wills …");

    worterbuch.apply_all_grave_goods_and_last_wills().await;

    if config.use_persistence {
        info!("Waiting for persistence hook to complete …");
        persistence::synchronous(worterbuch, &config).await?;
        info!("Shutdown persistence hook complete.");
    }

    Ok(())
}

async fn shutdown_servers(
    web_server: Option<ServerSubsystem>,
    tcp_server: Option<ServerSubsystem>,
    unix_socket: Option<ServerSubsystem>,
) {
    if let Some(it) = web_server {
        info!("Shutting down web server …");
        it.initiate_shutdown();
        if let Err(e) = it.join().await {
            error!("Error waiting for web server to shut down: {e}");
        }
    }

    if let Some(it) = tcp_server {
        info!("Shutting down tcp server …");
        it.initiate_shutdown();
        if let Err(e) = it.join().await {
            error!("Error waiting for tcp server to shut down: {e}");
        }
    }

    if let Some(it) = unix_socket {
        info!("Shutting down unix socket …");
        it.initiate_shutdown();
        if let Err(e) = it.join().await {
            error!("Error waiting for unix socket to shut down: {e}");
        }
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
        | WbFunction::Connected(_, _, _)
        | WbFunction::Disconnected(_, _)
        | WbFunction::Config(_)
        | WbFunction::Export(_)
        | WbFunction::Len(_)
        | WbFunction::Lock(_, _, _)
        | WbFunction::ReleaseLock(_, _, _) => None,
        WbFunction::Set(key, value, _, _) => {
            if !filter_sys || !key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX) {
                Some(ClientWriteCommand::Set(key.to_owned(), value.to_owned()))
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
}
