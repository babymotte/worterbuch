/*
 *  Helper functions for leader mode
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

mod cluster_sync_port;
mod virtual_proxy_server;

use crate::{
    Config, INTERNAL_CLIENT_ID, Worterbuch,
    cluster::{
        ClusterStateChangeReceiver, ClusterStateChangeSender, Mode, Servers,
        leader::cluster_sync_port::run_cluster_sync_port, process_api_call, protocol::StateSync,
        shutdown,
    },
    error::WorterbuchAppResult,
    server::common::{CloneableWbApi, WbFunction},
};
use serde_json::json;
use std::{net::SocketAddr, ops::ControlFlow};
use tokio::sync::{mpsc, oneshot};
use tosub::Subsystem;
use totils::while_select;
use tracing::{Level, info, instrument, trace};
use worterbuch_common::{
    protocol::v1::{InternalAction, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, Trace},
    topic,
};

pub(crate) async fn run(
    subsys: &Subsystem,
    mut worterbuch: Worterbuch,
    api: &CloneableWbApi,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    servers: Servers,
    sync_port: u16,
) -> WorterbuchAppResult<()> {
    #[cfg(feature = "commercial")]
    if !config.license.features.clustering {
        return Err(crate::error::WorterbuchAppError::NoLicense(
            "clustering".to_owned(),
        ));
    }

    info!("Running in LEADER mode.");

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Leader),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::Startup),
            true,
        )
        .await?;

    let mut client_write_txs: Vec<(usize, ClusterStateChangeSender, bool)> = vec![];
    let (follower_connected_tx, mut follower_connected_rx) = mpsc::channel::<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>(config.channel_buffer_size);
    let (follower_disconnected_tx, mut follower_disconnected_rx) =
        mpsc::channel::<SocketAddr>(config.channel_buffer_size);

    let mut tx_id = 0;
    // let mut dead = vec![];

    let cfg = config.clone();
    let wb = api.named("cluster-sync-port");
    subsys.spawn("cluster_sync_port", async move |s| {
        run_cluster_sync_port(
            s,
            cfg,
            wb,
            follower_connected_tx,
            follower_disconnected_tx,
            sync_port,
        )
        .await
    });

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = follower_connected_rx.recv() => forward_follower_connected(recv, &mut worterbuch, &mut client_write_txs, &config, &mut tx_id).await?,
        recv = follower_disconnected_rx.recv() => forward_follower_disconnected(recv, &mut worterbuch).await?,
        recv = api_rx.recv() => forward_api_call(recv, &mut worterbuch).await?,
    }

    info!("Main loop stopped, shutting down.");

    shutdown(subsys, worterbuch, config, servers).await
}

#[instrument(level = Level::TRACE, skip_all, err)]
async fn forward_api_call(
    recv: Option<WbFunction>,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<ControlFlow<()>> {
    trace!(enter = "forward_api_call");
    match recv {
        Some(function) => {
            process_api_call(worterbuch, function).await;
        }
        None => {
            trace!(exit = "forward_api_call");
            return Ok(ControlFlow::Break(()));
        }
    }
    trace!(exit = "forward_api_call");
    Ok(ControlFlow::Continue(()))
}

async fn forward_follower_connected(
    recv: Option<(
        oneshot::Sender<(StateSync, ClusterStateChangeReceiver)>,
        SocketAddr,
        bool,
    )>,
    worterbuch: &mut Worterbuch,
    client_write_txs: &mut Vec<(usize, ClusterStateChangeSender, bool)>,
    config: &Config,
    tx_id: &mut usize,
) -> WorterbuchAppResult<ControlFlow<()>> {
    trace!(enter = "forward_follower_connected");
    match recv {
        Some((state_tx, remote_addr, is_proxy)) => {
            let (client_write_tx, client_write_rx) = mpsc::channel(config.channel_buffer_size);
            let (current_state, grave_goods, last_wills) = worterbuch.export();
            let state_sync = StateSync {
                store: current_state,
                grave_goods,
                last_wills,
            };
            if state_tx.send((state_sync, client_write_rx)).is_ok() {
                client_write_txs.push((*tx_id, client_write_tx.clone(), is_proxy));
                *tx_id += 1;
            }
            worterbuch.follower_connected(remote_addr, client_write_tx, is_proxy);
            trace!(exit = "forward_follower_connected");
            Ok(ControlFlow::Continue(()))
        }
        None => {
            trace!(exit = "forward_follower_connected");
            Ok(ControlFlow::Break(()))
        }
    }
}

async fn forward_follower_disconnected(
    recv: Option<SocketAddr>,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<ControlFlow<()>> {
    trace!(enter = "forward_follower_disconnected");
    match recv {
        Some(remote_addr) => {
            worterbuch.follower_disconnected(remote_addr);
            trace!(exit = "forward_follower_disconnected");
            Ok(ControlFlow::Continue(()))
        }
        None => {
            trace!(exit = "forward_follower_disconnected");
            Ok(ControlFlow::Break(()))
        }
    }
}
