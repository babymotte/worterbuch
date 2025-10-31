/*
 *  Types and helper functions for leader/follower mode
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

use crate::{Config, INTERNAL_CLIENT_ID, Worterbuch, error::WorterbuchAppResult, store::StoreNode};
use miette::{Error, IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    io::{self, BufRead, ErrorKind},
    net::{IpAddr, SocketAddr},
    thread,
};
use tokio::{
    net::{TcpSocket, TcpStream},
    select,
    sync::{mpsc, oneshot},
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, error, info, trace};
use worterbuch_common::{
    CasVersion, GraveGoods, Key, LastWill, RequestPattern, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT,
    Value, topic, write_line_and_flush,
};

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mode {
    Leader,
    Follower,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaderSyncMessage {
    Init(StateSync),
    Mut(ClientWriteCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientWriteCommand {
    Set(Key, Value),
    CSet(Key, Value, CasVersion),
    Delete(Key),
    PDelete(RequestPattern),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSync(pub StoreNode, pub GraveGoods, pub LastWill);

pub async fn run_cluster_sync_port(
    subsys: &mut SubsystemHandle,
    config: Config,
    on_follower_connected: mpsc::Sender<
        oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    >,
) -> Result<()> {
    let port = config.sync_port.expect("no cluster sync port configured");
    let ip = config
        .tcp_endpoint
        .clone()
        .expect("no tcp bind address configured")
        .bind_addr;

    info!("Starting cluster sync endpoint at {}:{} …", ip, port);

    let socket = match ip {
        IpAddr::V4(_) => TcpSocket::new_v4().into_diagnostic()?,
        IpAddr::V6(_) => TcpSocket::new_v6().into_diagnostic()?,
    };

    socket.set_reuseaddr(true).into_diagnostic()?;
    #[cfg(target_family = "unix")]
    socket.set_reuseport(true).into_diagnostic()?;
    socket.bind(SocketAddr::new(ip, port)).into_diagnostic()?;

    let listener = socket.listen(1024).into_diagnostic()?;

    loop {
        select! {
            client = listener.accept() => match client {
                // TODO reject connections from clients that are not cluster peers
                Ok(client) => serve(subsys, client, &on_follower_connected, config.clone()).await,
                Err(e) => {
                    error!("Error accepting follower connections: {e}");
                    break;
                },
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    drop(listener);

    info!("Cluster sync port closed.");

    Ok(())
}

async fn serve(
    subsys: &SubsystemHandle,
    client: (TcpStream, SocketAddr),
    on_follower_connected: &mpsc::Sender<
        oneshot::Sender<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    >,
    config: Config,
) {
    info!("Follower {} connected.", client.1);
    let (sync_tx, sync_rx) = oneshot::channel();
    if on_follower_connected.send(sync_tx).await.is_err() {
        return;
    }

    subsys.start(SubsystemBuilder::new(
        client.1.to_string(),
        async move |s: &mut SubsystemHandle| {
            forward_events_to_follower(s, client.0, client.1, sync_rx, config).await;
            Ok::<(), Error>(())
        },
    ));
}

async fn forward_events_to_follower(
    subsys: &mut SubsystemHandle,
    mut tcp_stream: TcpStream,
    follower: SocketAddr,
    sync_rx: oneshot::Receiver<(StateSync, mpsc::Receiver<ClientWriteCommand>)>,
    config: Config,
) {
    let (state, mut commands) = match sync_rx.await {
        Ok(it) => it,
        Err(_) => return,
    };

    if let Err(e) = write_line_and_flush(
        LeaderSyncMessage::Init(state),
        &mut tcp_stream,
        config.send_timeout,
        follower,
    )
    .await
    {
        error!("Could not send current state to follower: {e}");
        return;
    }

    let mut buf = [0u8; 1024];

    loop {
        select! {
            recv = commands.recv() => match recv {
                Some(cmd) => if let Err(e) = write_line_and_flush(LeaderSyncMessage::Mut(cmd), &mut tcp_stream, config.send_timeout, follower).await {
                    error!("Could not write command to follower: {e}");
                    break;
                },
                None => break,
            },
            read = tcp_stream.readable() => {
                if let Err(e) = read {
                    error!("Follower {follower} closed the connection: {e}");
                    break;
                }
                match tcp_stream.try_read(&mut buf) {
                    Ok(0) => {
                        info!("Follower {follower} closed the connection.");
                        break;
                    }
                    Err(e) => {
                        if e.kind() != ErrorKind::WouldBlock {
                            error!("Follower {follower} closed the connection: {e}");
                            break;
                        }
                    }
                    Ok(_) => {
                        // follower actually wrote womething, but we don't care
                    }
                }
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    drop(tcp_stream);

    info!("TCP connection to follower {} closed.", follower);
}

pub async fn initial_sync(
    state_sync: StateSync,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
    worterbuch.reset_store(state_sync.0).await?;
    worterbuch
        .set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Follower),
            INTERNAL_CLIENT_ID,
        )
        .await?;
    Ok(())
}

pub async fn process_leader_message(
    msg: LeaderSyncMessage,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
    trace!("Received leader sync message: {msg:?}");

    match msg {
        LeaderSyncMessage::Init(_) => {
            return Err(crate::error::WorterbuchAppError::ClusterError(
                "already synced".to_owned(),
            ));
        }
        LeaderSyncMessage::Mut(client_write_command) => match client_write_command {
            ClientWriteCommand::Set(key, value) => {
                worterbuch.set(key, value, INTERNAL_CLIENT_ID).await?;
            }
            ClientWriteCommand::CSet(key, value, versions) => {
                worterbuch
                    .cset(key, value, versions, INTERNAL_CLIENT_ID)
                    .await?;
            }
            ClientWriteCommand::Delete(key) => {
                worterbuch.delete(key, INTERNAL_CLIENT_ID).await?;
            }
            ClientWriteCommand::PDelete(pattern) => {
                worterbuch.pdelete(pattern, INTERNAL_CLIENT_ID).await?;
            }
        },
    }

    Ok(())
}

pub fn shutdown_on_stdin_close(subsys: &SubsystemHandle) {
    info!("Registering stdin close handler …");

    let (tx, rx) = oneshot::channel();

    subsys.start(SubsystemBuilder::new(
        "stdin-monitor",
        async |s: &mut SubsystemHandle| {
            select! {
                _ = rx => (),
                _ = s.on_shutdown_requested() => (),
            }
            info!("Shutting down …");
            s.request_shutdown();
            Ok::<(), miette::Error>(())
        },
    ));

    thread::spawn(move || {
        let stdin = io::stdin();
        let handle = stdin.lock().lines();

        for line in handle {
            // ignore
            debug!("{line:?}");
        }

        info!("stdin closed.");
        tx.send(()).ok();
    });
}
