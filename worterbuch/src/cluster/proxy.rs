/*
 *  Types and helper functions for proxy mode
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

use crate::{
    Config, Servers,
    cluster::{
        self, LeaderState, Mode,
        protocol::{
            ClientWriteCommand, ClusterStateChange, Connected, Disconnected, Handshake,
            LeaderMessage, LeaderWelcome, Locks, ProxyHandshake, ProxyMessage, Request,
        },
        shutdown,
    },
    error::{WorterbuchAppError, WorterbuchAppResult},
    persistence::unlock_persistence,
    server::common::WbFunction,
    store::StoreNode,
    worterbuch::Worterbuch,
    worterbuch_version,
};
use hashbrown::HashMap;
use serde_json::json;
use std::{future::pending, ops::ControlFlow, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select, spawn,
    sync::{mpsc, oneshot},
};
use tosub::Subsystem;
use totils::{CancelOn, while_select};
use tracing::{debug, error, info, trace, warn};
use worterbuch_common::{
    ClientId, INTERNAL_CLIENT_ID, LockLostSender,
    error::{ConnectionResult, WorterbuchResult},
    is_grave_goods_topic, is_last_will_topic,
    protocol::v1::{
        CSet, ClientMessage, Delete, ErrorCode, InternalAction, Key, KeyValuePairs, Lock, PDelete,
        PStateEvent, ProtocolSwitchRequest, Publish, SPub, SPubInit, SYSTEM_TOPIC_CLUSTER,
        SYSTEM_TOPIC_LEADER, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, ServerMessage, Set, StateEvent,
        Trace, TransactionId, Value,
    },
    receive_msg, topic, write_line_and_flush,
};

struct RunResult {
    initial_connection_successful: bool,
    leader_addresses_updated: bool,
}

struct Proxy {
    subsys: Subsystem,
    worterbuch: Worterbuch,
    api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    servers: Servers,
    leader_addresses: Box<[String]>,
    stdin: mpsc::Receiver<String>,
    counter: usize,
    retry_seconds: u64,
    locks: Locks,
    response_interests: HashMap<ClientId, ClientResponseInterests>,
    read_leader_addresses_from_stdin: bool,
}

impl Proxy {
    fn new(
        subsys: Subsystem,
        worterbuch: Worterbuch,
        api_rx: mpsc::Receiver<WbFunction>,
        config: Config,
        servers: Servers,
        leader_addresses: Box<[String]>,
        stdin: mpsc::Receiver<String>,
    ) -> WorterbuchAppResult<Self> {
        #[cfg(feature = "commercial")]
        if !config.license.features.proxy {
            return Err(crate::error::WorterbuchAppError::NoLicense(
                "proxy".to_owned(),
            ));
        }

        // TODO get from config
        let retry_seconds = 1;
        let counter = 0;
        let locks = Locks::default();
        let response_interests = HashMap::new();

        Ok(Proxy {
            subsys,
            worterbuch,
            api_rx,
            config,
            servers,
            leader_addresses,
            stdin,
            retry_seconds,
            counter,
            locks,
            response_interests,
            // TODO get from config
            read_leader_addresses_from_stdin: true,
        })
    }

    async fn run(mut self) -> WorterbuchAppResult<()> {
        info!(
            "Running in PROXY mode. Leaders: {:?}",
            self.leader_addresses
        );

        self.worterbuch
            .internal_set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
                json!(Mode::Proxy),
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::Startup),
                true,
            )
            .await?;

        self.main_loop().await?;

        info!("Main loop stopped, shutting down.");

        shutdown(&self.subsys, self.worterbuch, self.config, self.servers).await
    }

    async fn main_loop(&mut self) -> Result<(), WorterbuchAppError> {
        'outer: loop {
            trace!("entering main loop body");
            self.counter = 0;
            self.worterbuch
                .internal_set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
                    json!(&LeaderState::Disconnected),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::LeaderSync),
                    true,
                )
                .await?;

            trace!(
                leader_addresses = ?self.leader_addresses,
                "checking configured leader addresses"
            );
            if self.leader_addresses.is_empty() {
                warn!(
                    "No leader addresses provided. Waiting to receive new list of leader addresses from stdin …"
                );
                if self.read_leader_addresses_from_stdin
                    && let Some(recv) = read_stdin(
                        &mut self.stdin,
                        &mut self.read_leader_addresses_from_stdin,
                        self.subsys.shutdown_requested(),
                    )
                    .await
                {
                    trace!(?recv, "received data on stdin");
                    if self.read_leader_addresses_from_stdin
                        && update_leader_addresses(
                            Some(recv),
                            &mut self.leader_addresses,
                            self.read_leader_addresses_from_stdin,
                        )
                    {
                        continue 'outer;
                    }
                } else {
                    break 'outer;
                }
            }

            loop {
                trace!(counter = self.counter, "entering inner loop body");

                for leader_address in self.leader_addresses.clone() {
                    if self.read_leader_addresses_from_stdin
                        && update_leader_addresses(
                            self.stdin.try_recv().ok(),
                            &mut self.leader_addresses,
                            self.read_leader_addresses_from_stdin,
                        )
                    {
                        continue 'outer;
                    }

                    trace!(
                        leader_address,
                        counter = self.counter,
                        "leader address selected"
                    );
                    self.worterbuch
                        .internal_set(
                            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
                            json!(&LeaderState::Disconnected),
                            INTERNAL_CLIENT_ID,
                            Trace::InternalAction(InternalAction::LeaderSync),
                            true,
                        )
                        .await?;

                    let shutdown_requested = self.subsys.clone().into_shutdown_requested();
                    let Some(res) = self
                        .run_with_leader(leader_address.clone())
                        .or_cancel_on(shutdown_requested)
                        .await
                    else {
                        break 'outer;
                    };
                    let res = res?;
                    if res.leader_addresses_updated {
                        continue 'outer;
                    } else {
                        self.counter += 1;
                        if res.initial_connection_successful {
                            info!(
                                "Connection to leader {} lost. Trying next leader …",
                                leader_address
                            );
                            self.counter = 0;
                        } else {
                            info!(
                                "Could not connect to leader {}. Trying next leader …",
                                leader_address
                            );
                        }
                    }

                    if self.counter >= self.leader_addresses.len() {
                        self.counter = 0;
                        warn!(
                            "Could not connect to any leader. Retrying in {} second(s) …",
                            self.retry_seconds
                        );
                        select! {
                            biased;
                            _ = self.subsys.shutdown_requested() => break 'outer,
                            _ = tokio::time::sleep(Duration::from_secs(self.retry_seconds)) => {
                                trace!("timeout elapsed, re-trying leader connections");
                            },
                            recv = read_stdin(&mut self.stdin, &mut self.read_leader_addresses_from_stdin, self.subsys.shutdown_requested()) => {
                                if self.read_leader_addresses_from_stdin && update_leader_addresses(recv, &mut self.leader_addresses, self.read_leader_addresses_from_stdin) {
                                    continue 'outer;
                                }
                            },
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn run_with_leader(&mut self, leader_address: String) -> WorterbuchAppResult<RunResult> {
        trace!(enter = "run_with_leader", leader_address);
        self.worterbuch
            .internal_set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
                json!(LeaderState::Connecting(leader_address.clone())),
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::LeaderSync),
                true,
            )
            .await?;

        let stream = match TcpStream::connect(leader_address.clone()).await {
            Ok(it) => it,
            Err(e) => {
                warn!("Failed to connect to leader {}: {}", leader_address, e);
                trace!(exit = "run_with_leader", leader_address);
                return Ok(RunResult {
                    leader_addresses_updated: false,
                    initial_connection_successful: false,
                });
            }
        };
        let (leader_rx, leader_tx) = stream.into_split();
        let mut lines = BufReader::new(leader_rx).lines();

        let proxy_request_tx = init_request_sender(
            &self.subsys,
            leader_tx,
            &self.config,
            leader_address.clone(),
        );

        let timeout = Some(self.config.initial_sync_timeout);

        if let Err(result) = self
            .establish_leader_session(
                leader_address.clone(),
                &mut lines,
                &proxy_request_tx,
                timeout,
            )
            .await?
        {
            trace!(exit = "run_with_leader", leader_address);
            return Ok(result);
        }

        let leader_connection = LeaderConnection::new(
            &self.subsys,
            proxy_request_tx,
            &mut self.worterbuch,
            &mut self.api_rx,
            lines,
            &self.config,
            leader_address.clone(),
            &mut self.locks,
            &mut self.response_interests,
            &mut self.leader_addresses,
            &mut self.stdin,
            &mut self.read_leader_addresses_from_stdin,
        );

        let leader_addresses_updated = leader_connection.run().await?;

        info!(
            "Proxy loop for leader {} stopped, closing connection.",
            leader_address
        );

        trace!(exit = "run_with_leader", leader_address);
        Ok(RunResult {
            leader_addresses_updated,
            initial_connection_successful: true,
        })
    }

    async fn establish_leader_session(
        &mut self,
        leader_address: String,
        lines: &mut Lines<BufReader<OwnedReadHalf>>,
        proxy_request_tx: &mpsc::Sender<ProxyMessage>,
        timeout: Option<Duration>,
    ) -> WorterbuchAppResult<Result<(), RunResult>> {
        trace!(enter = "establish_leader_session", leader_address);
        info!("Successfully connected to leader {leader_address}. Performing handshake …");

        let welcome = match self
            .receive_welcome_message(leader_address.clone(), lines, timeout)
            .await?
        {
            Ok(welcome) => welcome,
            Err(result) => {
                trace!(exit = "establish_leader_session");
                return Ok(Err(result));
            }
        };

        self.send_handshake(welcome, proxy_request_tx).await?;

        if let Err(result) = self
            .sync_with_leader(leader_address.clone(), lines, timeout)
            .await?
        {
            trace!(exit = "establish_leader_session");
            return Ok(Err(result));
        }

        self.worterbuch
            .internal_set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
                json!(LeaderState::Synced(leader_address)),
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::LeaderSync),
                true,
            )
            .await?;

        trace!(exit = "establish_leader_session");
        Ok(Ok(()))
    }

    async fn receive_welcome_message(
        &mut self,
        leader_address: String,
        lines: &mut Lines<BufReader<OwnedReadHalf>>,
        timeout: Option<Duration>,
    ) -> WorterbuchAppResult<Result<LeaderWelcome, RunResult>> {
        trace!(enter = "receive_welcome_message", leader_address);

        self.worterbuch
            .internal_set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
                json!(LeaderState::Handshake(leader_address.clone())),
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::LeaderSync),
                true,
            )
            .await?;
        debug!("Receiving welcome message from leader …");
        let welcome = loop {
            select! {
                biased;
                _ = self.subsys.shutdown_requested() => {
                    warn!("Shutdown requested before receiving leader welcome message.");
                    trace!(exit = "receive_welcome_message");
                    return Err(WorterbuchAppError::ClusterError("shut down before receiving leader welcome message".to_owned()));
                },
                recv = read_stdin(&mut self.stdin, &mut self.read_leader_addresses_from_stdin, self.subsys.shutdown_requested()) => {
                    if self.read_leader_addresses_from_stdin && update_leader_addresses(recv, &mut self.leader_addresses, self.read_leader_addresses_from_stdin) {
                        if !self.leader_addresses.contains(&leader_address) {
                            warn!(
                                "Current leader address {} is no longer in the list of known leader addresses {:?}",
                                leader_address, self.leader_addresses
                            );
                            trace!(exit = "receive_welcome_message");
                            return Ok(Err(RunResult {
                                leader_addresses_updated: true,
                                initial_connection_successful: false,
                            }));
                        }
                    }
                },
                recv = receive_msg(lines, timeout) => {
                    debug!("Received leader message");
                    match recv {
                        Ok(Some(msg)) => {
                            if let LeaderMessage::Welcome(welcome) = msg {
                                debug!("Received welcome message from leader: {welcome:?}");
                                break welcome;
                            } else {
                                warn!("Expected welcome message from leader, but got: {msg:?}");
                                trace!(exit = "receive_welcome_message");
                                return Ok(Err(RunResult {
                                    leader_addresses_updated: false,
                                    initial_connection_successful: false,
                                }));
                            }
                        },
                        Ok(None) => {
                            warn!("Leader closed connection before sending welcome message.");
                            trace!(exit = "receive_welcome_message");
                            return Ok(Err(RunResult {
                                leader_addresses_updated: false,
                                initial_connection_successful: false,
                            }));
                        },
                        Err(e) => {
                            warn!("Error receiving welcome message from leader: {e}");
                            trace!(exit = "receive_welcome_message");
                            return Ok(Err(RunResult {
                                leader_addresses_updated: false,
                                initial_connection_successful: false,
                            }));
                        }
                    }
                },
            };
        };

        trace!(exit = "receive_welcome_message");
        Ok(Ok(welcome))
    }

    async fn send_handshake(
        &self,
        welcome: LeaderWelcome,
        proxy_request_tx: &mpsc::Sender<ProxyMessage>,
    ) -> WorterbuchAppResult<()> {
        trace!(enter = "send_handshake", ?welcome);

        let version = worterbuch_version();
        let auth_token = if welcome.authentication_required {
            // TODO
            None
        } else {
            None
        };

        let connected_clients = connected_clients(&self.worterbuch);

        let handshake = ProxyMessage::Handshake(Handshake::Proxy(ProxyHandshake {
            version,
            auth_token,
            locks: self.locks.clone(),
            connected_clients,
        }));

        proxy_request_tx.send(handshake).await?;

        trace!(exit = "send_handshake");
        Ok(())
    }

    async fn sync_with_leader(
        &mut self,
        leader_address: String,
        lines: &mut Lines<BufReader<OwnedReadHalf>>,
        timeout: Option<Duration>,
    ) -> WorterbuchAppResult<Result<(), RunResult>> {
        trace!(enter = "sync_with_leader", leader_address);
        info!("Handshake complete. Waiting for initial sync message …");

        self.worterbuch
            .internal_set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
                json!(LeaderState::Syncing(leader_address.clone())),
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::LeaderSync),
                true,
            )
            .await?;
        let mut persistence_interval = self.config.persistence_interval();
        loop {
            select! {
                biased;
                _ = self.subsys.shutdown_requested() => {
                    warn!("Shutdown requested before initial sync completed.");
                    trace!(exit = "sync_with_leader");
                    return Err(WorterbuchAppError::ClusterError("shut down before initial sync".to_owned()));
                },
                recv = read_stdin(&mut self.stdin, &mut self.read_leader_addresses_from_stdin, self.subsys.shutdown_requested()) => {
                    if self.read_leader_addresses_from_stdin && update_leader_addresses(recv, &mut self.leader_addresses, self.read_leader_addresses_from_stdin) {
                        if !self.leader_addresses.contains(&leader_address) {
                            warn!(
                                "Current leader address {} is no longer in the list of known leader addresses {:?}",
                                leader_address, self.leader_addresses
                            );
                            trace!(exit = "sync_with_leader");
                            return Ok(Err(RunResult {
                                leader_addresses_updated: true,
                                initial_connection_successful: false,
                            }));
                        }
                    }
                },
                recv = receive_msg(lines, timeout) => {
                    debug!("Received leader message");
                    match recv {
                        Ok(Some(msg)) => {
                            if let LeaderMessage::Init(state) = msg {
                                debug!("Received initial sync message from leader: {state:?}");
                                self.initial_sync(state.store).await?;
                                persistence_interval.reset();
                                self.worterbuch.flush().await?;
                                break;
                            } else {
                                warn!("Expected initial sync message from leader, but got: {msg:?}");
                                trace!(exit = "sync_with_leader");
                                return Ok(Err(RunResult {
                                    leader_addresses_updated: false,
                                    initial_connection_successful: false,
                                }));
                            }
                        },
                        Ok(None) => {
                            warn!("Leader closed connection before sending initial sync message.");
                            trace!(exit = "sync_with_leader");
                            return Ok(Err(RunResult {
                                leader_addresses_updated: false,
                                initial_connection_successful: false,
                            }));
                        },
                        Err(e) => {
                            warn!("Error receiving initial sync message from leader: {e}");
                            trace!(exit = "sync_with_leader");
                            return Ok(Err(RunResult {
                                leader_addresses_updated: false,
                                initial_connection_successful: false,
                            }));
                        }
                    }
                },
            };
        }

        info!("Successfully synced with leader.");

        trace!(exit = "sync_with_leader");
        Ok(Ok(()))
    }

    async fn initial_sync(&mut self, store: StoreNode) -> WorterbuchAppResult<()> {
        trace!(enter = "initial_sync");

        self.worterbuch
            .reset_store_and_notify_subscribers(store, true)
            .await?;

        self.worterbuch
            .internal_set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
                json!(Mode::Proxy),
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::LeaderSync),
                true,
            )
            .await?;

        unlock_persistence();

        self.worterbuch.flush().await.map_err(|e| {
            WorterbuchAppError::ClusterError(format!(
                "Failed to flush storage after initial sync: {e}"
            ))
        })?;

        trace!(enter = "initial_sync");
        Ok(())
    }
}

pub(crate) async fn run(
    subsys: &Subsystem,
    worterbuch: Worterbuch,
    api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    servers: Servers,
    leader_addresses: Box<[String]>,
    stdin: mpsc::Receiver<String>,
) -> WorterbuchAppResult<()> {
    subsys
        .spawn("proxy", |s| async {
            Proxy::new(
                s,
                worterbuch,
                api_rx,
                config,
                servers,
                leader_addresses,
                stdin,
            )?
            .run()
            .await?;
            Ok::<(), WorterbuchAppError>(())
        })
        .join()
        .await?;

    Ok(())
}

async fn read_stdin(
    stdin: &mut mpsc::Receiver<String>,
    enabled: &mut bool,
    shutdown_requested: impl Future,
) -> Option<String> {
    if !*enabled {
        pending().or_cancel_on(shutdown_requested).await
    } else {
        match stdin
            .recv()
            .or_cancel_on(shutdown_requested)
            .await
            .flatten()
        {
            Some(line) => Some(line),
            None => {
                trace!("stdin closed, disabling further input from stdin");
                *enabled = false;
                None
            }
        }
    }
}

fn update_leader_addresses(
    new_addresses: Option<String>,
    leader_addresses: &mut Box<[String]>,
    enabled: bool,
) -> bool {
    trace!(enter = "update_leader_addresses", ?new_addresses);

    if !enabled {
        panic!(
            "this is not supposed to be called if reading leader addresses from stdin is disabled"
        );
    }

    let Some(new_addresses) = new_addresses else {
        return false;
    };

    if new_addresses.trim().is_empty() {
        trace!(
            exit = "update_leader_addresses",
            "Read empty line from stdin"
        );
        return false;
    }

    let addresses = match serde_json::from_str::<Box<[String]>>(&new_addresses) {
        Ok(addresses) => {
            trace!(
                ?addresses,
                "Successfully deserialized new addresses addresses"
            );
            addresses
        }
        Err(e) => {
            error!("Could not parse address array '{}': {}", new_addresses, e);
            trace!(exit = "update_leader_addresses");
            return false;
        }
    };

    *leader_addresses = addresses;
    info!("Updated leader addresses: {:?}", leader_addresses);

    trace!(exit = "update_leader_addresses");
    true
}

#[derive(Debug, Default)]
struct ClientResponseInterests {
    ack: HashMap<TransactionId, (ClientMessage, oneshot::Sender<WorterbuchResult<()>>)>,
    state: HashMap<TransactionId, (ClientMessage, oneshot::Sender<WorterbuchResult<Value>>)>,
    pstate: HashMap<
        TransactionId,
        (
            ClientMessage,
            oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
        ),
    >,
    lock_acquired: HashMap<
        TransactionId,
        (
            ClientMessage,
            oneshot::Sender<WorterbuchResult<()>>,
            LockLostSender,
        ),
    >,
    lock_lost: HashMap<TransactionId, LockLostSender>,
}

impl ClientResponseInterests {
    fn is_empty(&self) -> bool {
        self.state.is_empty()
            && self.pstate.is_empty()
            && self.lock_acquired.is_empty()
            && self.lock_lost.is_empty()
            && self.ack.is_empty()
    }
}

struct LeaderConnection<'a> {
    leader_address: String,
    subsys: &'a Subsystem,
    response_interests: &'a mut HashMap<ClientId, ClientResponseInterests>,
    proxy_request_tx: mpsc::Sender<ProxyMessage>,
    worterbuch: &'a mut Worterbuch,
    api_rx: &'a mut mpsc::Receiver<WbFunction>,
    lines: Lines<BufReader<OwnedReadHalf>>,
    locks: &'a mut Locks,
    leader_addresses: &'a mut Box<[String]>,
    leader_addresses_updated: bool,
    stdin: &'a mut mpsc::Receiver<String>,
    read_leader_addresses_from_stdin: &'a mut bool,
}

impl<'a> LeaderConnection<'a> {
    fn new(
        subsys: &'a Subsystem,
        proxy_request_tx: mpsc::Sender<ProxyMessage>,
        worterbuch: &'a mut Worterbuch,
        api_rx: &'a mut mpsc::Receiver<WbFunction>,
        lines: Lines<BufReader<OwnedReadHalf>>,
        config: &Config,
        leader_address: String,
        locks: &'a mut Locks,
        response_interests: &'a mut HashMap<ClientId, ClientResponseInterests>,
        leader_addresses: &'a mut Box<[String]>,
        stdin: &'a mut mpsc::Receiver<String>,
        read_leader_addresses_from_stdin: &'a mut bool,
    ) -> Self {
        Self {
            leader_address,
            subsys,
            response_interests,
            proxy_request_tx,
            worterbuch,
            api_rx,
            lines,
            locks,
            leader_addresses,
            leader_addresses_updated: false,
            stdin,
            read_leader_addresses_from_stdin,
        }
    }

    async fn run(mut self) -> WorterbuchAppResult<bool> {
        debug!(
            "Starting new leder session with inherited response interests: {:#?}",
            self.response_interests
        );

        while_select! {
            _ = self.subsys.shutdown_requested() => break,
            recv = read_stdin(self.stdin, self.read_leader_addresses_from_stdin, self.subsys.shutdown_requested()) => self.update_leader_address(recv),
            recv = receive_msg(&mut self.lines, None) => self.try_process_leader_message(recv).await?,
            recv = self.api_rx.recv() => self.try_process_api_call(recv).await?,
        }
        let leader_addresses_updated = self.leader_addresses_updated;

        Ok(leader_addresses_updated)
    }

    fn update_leader_address(&mut self, recv: Option<String>) -> ControlFlow<()> {
        if *self.read_leader_addresses_from_stdin
            && update_leader_addresses(
                recv,
                self.leader_addresses,
                *self.read_leader_addresses_from_stdin,
            )
        {
            self.leader_addresses_updated = true;
            if self.leader_addresses.contains(&self.leader_address) {
                ControlFlow::Continue(())
            } else {
                warn!(
                    "Current leader address {} is no longer in the list of known leader addresses {:?}",
                    self.leader_address, self.leader_addresses
                );
                ControlFlow::Break(())
            }
        } else {
            ControlFlow::Continue(())
        }
    }

    async fn try_process_leader_message(
        &mut self,
        recv: ConnectionResult<Option<LeaderMessage>>,
    ) -> WorterbuchAppResult<ControlFlow<()>> {
        match recv {
            Ok(Some(msg)) => {
                self.process_leader_message(msg).await?;
                Ok(ControlFlow::Continue(()))
            }
            Ok(None) => Ok(ControlFlow::Break(())),
            Err(e) => {
                error!("Error receiving update from leader: {e}");
                Ok(ControlFlow::Break(()))
            }
        }
    }

    async fn process_leader_message(&mut self, msg: LeaderMessage) -> WorterbuchAppResult<()> {
        debug!("Processing leader sync message: {msg:?}");

        let res = match msg {
            LeaderMessage::Welcome(_) => {
                return Err(crate::error::WorterbuchAppError::ClusterError(
                    "already received welcome message".to_owned(),
                ));
            }
            LeaderMessage::Init(_) => {
                return Err(crate::error::WorterbuchAppError::ClusterError(
                    "already synced".to_owned(),
                ));
            }
            LeaderMessage::Mut(ClusterStateChange { command, trace, .. }) => match command {
                ClientWriteCommand::Set(key, value, force) => {
                    self.worterbuch
                        .internal_set(
                            key,
                            value,
                            trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                            trace,
                            force,
                        )
                        .await
                }
                ClientWriteCommand::CSet(key, value, versions, force) => {
                    self.worterbuch
                        .internal_cset(
                            key,
                            value,
                            versions,
                            trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                            trace,
                            force,
                        )
                        .await
                }
                ClientWriteCommand::Delete(key) => self
                    .worterbuch
                    .internal_delete(key, trace.client_id().unwrap_or(INTERNAL_CLIENT_ID), trace)
                    .await
                    .map(|_| ()),
                ClientWriteCommand::PDelete(pattern) => self
                    .worterbuch
                    .internal_pdelete(
                        pattern,
                        trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                        trace,
                    )
                    .await
                    .map(|_| ()),
                ClientWriteCommand::Publish(key, value) => {
                    self.worterbuch
                        .internal_publish(
                            key,
                            value,
                            trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                            trace,
                        )
                        .await
                }
                ClientWriteCommand::Import(persisted_store) => self
                    .worterbuch
                    .internal_import(
                        persisted_store,
                        trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                        trace,
                    )
                    .await
                    .map(|_| ()),
            },
            LeaderMessage::ClientResponse(client_id, server_message) => {
                self.forward_leader_response(client_id, server_message)
                    .await
            }
            LeaderMessage::EjectClient(client_id) => self.eject_client(client_id).await,
        };

        if let Err(e) = res {
            error!("Error applying leader sync message: {e}");
        }

        Ok(())
    }

    async fn try_process_api_call(
        &mut self,
        recv: Option<WbFunction>,
    ) -> WorterbuchAppResult<ControlFlow<()>> {
        match recv {
            Some(function) => {
                self.process_api_call(function).await?;
                Ok(ControlFlow::Continue(()))
            }
            None => Ok(ControlFlow::Break(())),
        }
    }

    async fn process_api_call(&mut self, function: WbFunction) -> WorterbuchAppResult<()> {
        debug!("Processing API call: {function:?}");
        match function {
            WbFunction::Connected(client_id, addr, protocol, eject, tx) => {
                let request = ProxyMessage::Connected(Connected {
                    client_id,
                    protocol: protocol.clone(),
                });
                self.proxy_request_tx.send(request).await?;
                cluster::process_api_call(
                    self.worterbuch,
                    WbFunction::Connected(client_id, addr, protocol, eject, tx),
                )
                .await;
            }
            WbFunction::Disconnected(client_id, protocol, socket_addr) => {
                let grave_goods = self
                    .worterbuch
                    .grave_goods_for_client(&client_id)
                    .unwrap_or_default();
                let last_will = self
                    .worterbuch
                    .last_will_for_client(&client_id)
                    .unwrap_or_default();

                let request = ProxyMessage::Disconnected(Disconnected {
                    client_id,
                    protocol: protocol.clone(),
                    grave_goods,
                    last_will,
                });
                self.proxy_request_tx.send(request).await?;
                cluster::process_api_call(
                    self.worterbuch,
                    WbFunction::Disconnected(client_id, protocol, socket_addr),
                )
                .await;
                self.locks.client_disconnected(client_id);
                self.drop_response_interests(client_id);
            }
            WbFunction::ProtocolSwitched(client_id, interface, version) => {
                let client_message =
                    ClientMessage::ProtocolSwitchRequest(ProtocolSwitchRequest { version });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface: interface.clone(),
                });
                let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
                spawn(ack_rx);
                self.register_ack_interest(client_id, 0, client_message, ack_tx);
                self.proxy_request_tx.send(request).await?;
                cluster::process_api_call(
                    self.worterbuch,
                    WbFunction::ProtocolSwitched(client_id, interface, version),
                )
                .await;
            }
            WbFunction::Set(transaction_id, interface, key, value, client_id, tx, span) => {
                let is_grave_goods_or_last_will =
                    is_grave_goods_topic(&key) || is_last_will_topic(&key);
                if is_grave_goods_or_last_will {
                    cluster::process_api_call(
                        self.worterbuch,
                        WbFunction::Set(transaction_id, interface, key, value, client_id, tx, span),
                    )
                    .await;
                } else {
                    let client_message = ClientMessage::Set(Set {
                        transaction_id,
                        key,
                        value,
                    });
                    let request = ProxyMessage::Request(Request {
                        client_id,
                        msg: client_message.clone(),
                        interface,
                    });
                    self.register_ack_interest(client_id, transaction_id, client_message, tx);
                    self.proxy_request_tx.send(request).await?;
                }
            }
            WbFunction::CSet(transaction_id, interface, key, value, version, client_id, tx) => {
                let client_message = ClientMessage::CSet(CSet {
                    transaction_id,
                    key,
                    value,
                    version,
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, client_message, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::SPubInit(transaction_id, interface, key, client_id, tx) => {
                let client_message = ClientMessage::SPubInit(SPubInit {
                    transaction_id,
                    key,
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, client_message, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::SPub(transaction_id, interface, value, client_id, tx) => {
                let client_message = ClientMessage::SPub(SPub {
                    transaction_id,
                    value,
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, client_message, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::Publish(transaction_id, interface, key, value, client_id, tx) => {
                let client_message = ClientMessage::Publish(Publish {
                    transaction_id,
                    key,
                    value,
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, client_message, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::Delete(transaction_id, interface, key, client_id, tx) => {
                let client_message = ClientMessage::Delete(Delete {
                    transaction_id,
                    key,
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                self.register_state_interest(client_id, transaction_id, client_message, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::PDelete(
                transaction_id,
                interface,
                request_pattern,
                quiet,
                client_id,
                tx,
            ) => {
                let client_message = ClientMessage::PDelete(PDelete {
                    transaction_id,
                    request_pattern,
                    quiet,
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                self.register_pstate_interest(client_id, transaction_id, client_message, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::Lock(transaction_id, interface, key, client_id, tx) => {
                let client_message = ClientMessage::Lock(Lock {
                    transaction_id,
                    key: key.clone(),
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                let (ack_tx, ack_rx) = oneshot::channel();
                let (lost_tx, lost_rx) = oneshot::channel();
                spawn(async move {
                    if let Ok(res) = ack_rx.await {
                        match res {
                            Ok(_) => {
                                trace!(
                                    "Lock acquired for client {}, transaction {}",
                                    client_id, transaction_id
                                );
                                tx.send(Ok(lost_rx)).ok();
                            }
                            Err(e) => {
                                trace!(
                                    "Lock acquisition failed for client {}, transaction {}: {:?}",
                                    client_id, transaction_id, e
                                );
                                tx.send(Err(e)).ok();
                            }
                        }
                    }
                });
                self.register_lock_acquired_interest(
                    client_id,
                    key.clone(),
                    transaction_id,
                    client_message,
                    ack_tx,
                    lost_tx,
                    false,
                );
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::AcquireLock(transaction_id, interface, key, client_id, tx) => {
                let client_message = ClientMessage::AcquireLock(Lock {
                    transaction_id,
                    key: key.clone(),
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                let (ack_tx, ack_rx) = oneshot::channel();
                let (acked_tx, acked_rx) = oneshot::channel();
                let (lost_tx, lost_rx) = oneshot::channel();
                spawn(async move {
                    if ack_rx.await.is_ok() {
                        acked_tx.send(()).ok();
                    }
                });
                self.register_lock_acquired_interest(
                    client_id,
                    key.clone(),
                    transaction_id,
                    client_message,
                    ack_tx,
                    lost_tx,
                    true,
                );
                self.proxy_request_tx.send(request).await?;
                tx.send(Ok((acked_rx, lost_rx))).ok();
            }
            WbFunction::ReleaseLock(transaction_id, interface, key, client_id, tx) => {
                let client_message = ClientMessage::ReleaseLock(Lock {
                    transaction_id,
                    key,
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                self.locks.released(client_id, transaction_id);
                let _ = self.get_lock_acquired_interest(client_id, transaction_id);
                let _ = self.get_lock_lost_interest(client_id, transaction_id);
                self.register_ack_interest(client_id, transaction_id, client_message, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::Import(_, _, _, _, _) => {
                warn!("Import not yet implemented");
                // TODO forward to leader
                // TODO register response interest
            }
            function => cluster::process_api_call(self.worterbuch, function).await,
        };

        Ok(())
    }

    async fn forward_leader_response(
        &mut self,
        client_id: ClientId,
        server_message: ServerMessage,
    ) -> WorterbuchResult<()> {
        match server_message {
            ServerMessage::Welcome(welcome) => {
                warn!("Received unexpected welcome message from leader: {welcome:?}");
            }
            ServerMessage::CState(cstate) => {
                warn!("Received unexpected CState message from leader: {cstate:?}");
            }

            ServerMessage::LsState(ls_state) => {
                warn!("Received unexpected LsState message from leader: {ls_state:?}");
            }
            ServerMessage::Authorized(ack) => {
                // TODO handle this correctly
                warn!(
                    "Received Authorized message from leader; handler not yet implemented: {ack:?}"
                );
            }
            ServerMessage::Ack(ack) => {
                if let Some(tx) = self.get_ack_response_interest(client_id, ack.transaction_id) {
                    tx.send(Ok(())).ok();
                } else if let Some((tx, lost_tx)) =
                    self.get_lock_acquired_interest(client_id, ack.transaction_id)
                {
                    self.locks.acquired(client_id, ack.transaction_id);
                    self.register_lock_lost_interest(client_id, ack.transaction_id, lost_tx);
                    tx.send(Ok(())).ok();
                } else {
                    warn!(
                        "Received Ack message from leader for client {client_id} but client did not register an interest: {ack:?}"
                    );
                }
            }
            ServerMessage::State(state) => match state.event {
                StateEvent::Value(value) => {
                    warn!("Received unexpected StateEvent::Value message from leader: {value:?}");
                }
                StateEvent::Deleted(value) => {
                    if let Some(tx) =
                        self.get_state_response_interest(client_id, state.transaction_id)
                    {
                        tx.send(Ok(value)).ok();
                    }
                }
            },
            ServerMessage::PState(pstate) => match pstate.event {
                PStateEvent::KeyValuePairs(kvps) => {
                    warn!(
                        "Received unexpected PState::KeyValuePairs message from leader: {kvps:?}"
                    );
                }
                PStateEvent::Deleted(kvps) => {
                    if let Some(tx) =
                        self.get_pstate_response_interest(client_id, pstate.transaction_id)
                    {
                        tx.send(Ok(kvps)).ok();
                    }
                }
            },
            ServerMessage::Err(e) => {
                warn!("Received error message from leader for client {client_id}: {e:?}");
                let transaction_id = e.transaction_id;
                let code = e.error_code;

                let mut e = Some(e);

                if code == ErrorCode::LockLost {
                    warn!(
                        "Received lock lost message for client {}, transaction {}",
                        client_id, transaction_id
                    );

                    let _ = self.get_lock_acquired_interest(client_id, transaction_id);

                    if let Some(tx) = self.get_lock_lost_interest(client_id, transaction_id) {
                        trace!(
                            "Forwarding lock lost notification to client {}, transaction {}",
                            client_id, transaction_id
                        );

                        let e = e.take();
                        debug_assert!(e.is_some(), "multiple interests registered for same error");
                        if e.is_some() {
                            tx.send(()).ok();
                        }
                    }
                }

                if let Some((tx, _)) = self.get_lock_acquired_interest(client_id, transaction_id) {
                    self.locks.acquisition_failed(client_id, transaction_id);
                    let e = e.take();
                    debug_assert!(e.is_some(), "multiple interests registered for same error");
                    if let Some(e) = e {
                        let e = Err(e.into());
                        trace!("{:#?}", e);
                        tx.send(e).ok();
                    }
                }

                if let Some(tx) = self.get_ack_response_interest(client_id, transaction_id) {
                    let e = e.take();
                    debug_assert!(e.is_some(), "multiple interests registered for same error");
                    if let Some(e) = e {
                        let e = Err(e.into());
                        trace!("{:#?}", e);
                        tx.send(e).ok();
                    }
                }

                if let Some(tx) = self.get_state_response_interest(client_id, transaction_id) {
                    let e = e.take();
                    debug_assert!(e.is_some(), "multiple interests registered for same error");
                    if let Some(e) = e {
                        let e = Err(e.into());
                        trace!("{:#?}", e);
                        tx.send(e).ok();
                    }
                }

                if let Some(tx) = self.get_pstate_response_interest(client_id, transaction_id) {
                    let e = e.take();
                    debug_assert!(e.is_some(), "multiple interests registered for same error");
                    if let Some(e) = e {
                        let e = Err(e.into());
                        trace!("{:#?}", e);
                        tx.send(e).ok();
                    }
                }
            }
            ServerMessage::LockLost(_) => {
                warn!("Received unexpected LockLost message from leader");
            }
        }

        Ok(())
    }

    async fn eject_client(&mut self, client_id: ClientId) -> WorterbuchResult<()> {
        warn!("Disconnecting client {client_id}");
        self.worterbuch.eject_client(client_id).await;
        Ok(())
    }

    fn register_ack_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        client_message: ClientMessage,
        tx: oneshot::Sender<WorterbuchResult<()>>,
    ) {
        trace!("Registering ack interest for client {client_id}, transaction {transaction_id}");

        self.response_interests
            .entry(client_id)
            .or_default()
            .ack
            .insert(transaction_id, (client_message, tx));

        trace!("ClientResponseInterests: {:#?}", self.response_interests);
    }

    fn register_state_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        client_message: ClientMessage,
        tx: oneshot::Sender<WorterbuchResult<Value>>,
    ) {
        trace!("Registering state interest for client {client_id}, transaction {transaction_id}");

        self.response_interests
            .entry(client_id)
            .or_default()
            .state
            .insert(transaction_id, (client_message, tx));

        trace!("ClientResponseInterests: {:#?}", self.response_interests);
    }

    fn register_pstate_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        client_message: ClientMessage,
        tx: oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ) {
        trace!("Registering pstate interest for client {client_id}, transaction {transaction_id}");

        self.response_interests
            .entry(client_id)
            .or_default()
            .pstate
            .insert(transaction_id, (client_message, tx));

        trace!("ClientResponseInterests: {:#?}", self.response_interests);
    }

    fn register_lock_acquired_interest(
        &mut self,
        client_id: ClientId,
        key: Key,
        transaction_id: TransactionId,
        client_message: ClientMessage,
        tx: oneshot::Sender<WorterbuchResult<()>>,
        lost_tx: LockLostSender,
        wait_for_lock: bool,
    ) {
        trace!(
            "Registering lock acquired interest for client {client_id}, transaction {transaction_id}, key {key:?}"
        );

        self.locks
            .requested(client_id, transaction_id, key, wait_for_lock);

        self.response_interests
            .entry(client_id)
            .or_default()
            .lock_acquired
            .insert(transaction_id, (client_message, tx, lost_tx));

        trace!("ClientResponseInterests: {:#?}", self.response_interests);
    }

    fn register_lock_lost_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        tx: LockLostSender,
    ) {
        trace!(
            "Registering lock lost interest for client {client_id}, transaction {transaction_id}"
        );

        self.response_interests
            .entry(client_id)
            .or_default()
            .lock_lost
            .insert(transaction_id, tx);

        trace!("ClientResponseInterests: {:#?}", self.response_interests);
    }

    fn drop_response_interests(&mut self, client_id: ClientId) {
        trace!("Dropping response interests for client {client_id}");

        self.response_interests.remove(&client_id);

        trace!("ClientResponseInterests: {:#?}", self.response_interests);
    }

    fn get_ack_response_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<oneshot::Sender<WorterbuchResult<()>>> {
        trace!(
            "Getting ack response interest for client {client_id}, transaction {transaction_id}"
        );

        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests.ack.remove(&transaction_id).map(|(_, tx)| tx);
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

        trace!("found: {}", tx.is_some());
        trace!("ClientResponseInterests: {:#?}", self.response_interests);

        tx
    }

    fn get_lock_acquired_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<(oneshot::Sender<WorterbuchResult<()>>, LockLostSender)> {
        trace!(
            "Getting lock acquired interest for client {client_id}, transaction {transaction_id}"
        );

        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests
            .lock_acquired
            .remove(&transaction_id)
            .map(|(_, tx, lost_tx)| (tx, lost_tx));
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

        trace!("found: {}", tx.is_some());
        trace!("ClientResponseInterests: {:#?}", self.response_interests);

        tx
    }

    fn get_lock_lost_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<LockLostSender> {
        trace!("Getting lock lost interest for client {client_id}, transaction {transaction_id}");

        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests.lock_lost.remove(&transaction_id);
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

        trace!("found: {}", tx.is_some());
        trace!("ClientResponseInterests: {:#?}", self.response_interests);

        tx
    }

    fn get_state_response_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<oneshot::Sender<WorterbuchResult<Value>>> {
        trace!(
            "Getting state response interest for client {client_id}, transaction {transaction_id}"
        );

        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests.state.remove(&transaction_id).map(|(_, tx)| tx);
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

        trace!("found: {}", tx.is_some());
        trace!("ClientResponseInterests: {:#?}", self.response_interests);

        tx
    }

    fn get_pstate_response_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<oneshot::Sender<WorterbuchResult<KeyValuePairs>>> {
        trace!(
            "Getting pstate response interest for client {client_id}, transaction {transaction_id}"
        );

        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests.pstate.remove(&transaction_id).map(|(_, tx)| tx);
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

        trace!("found: {}", tx.is_some());
        trace!("ClientResponseInterests: {:#?}", self.response_interests);

        tx
    }
}

fn connected_clients(worterbuch: &Worterbuch) -> Vec<Connected> {
    worterbuch
        .clients()
        .iter()
        .map(|(client_id, client_info)| Connected {
            client_id: *client_id,
            protocol: client_info.protocol.clone(),
        })
        .collect()
}

fn init_request_sender(
    subsys: &Subsystem,
    leader_tx: OwnedWriteHalf,
    config: &Config,
    leader_addr: String,
) -> mpsc::Sender<ProxyMessage> {
    trace!("Initializing request sender …");
    let (tx, rx) = mpsc::channel(config.channel_buffer_size);
    let send_timeout = config.send_timeout;
    subsys.spawn("proxy_request_sender", move |s| {
        request_sender_loop(s, leader_tx, rx, send_timeout, leader_addr)
    });
    tx
}

async fn request_sender_loop(
    subsys: Subsystem,
    mut leader_tx: OwnedWriteHalf,
    mut rx: mpsc::Receiver<ProxyMessage>,
    timeout: Option<Duration>,
    leader_addr: String,
) -> miette::Result<()> {
    trace!("Request sender loop running, waiting for messages to write to socket …");
    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = rx.recv() => forward_client_request(&subsys, recv, &mut leader_tx, timeout, leader_addr.clone()).await,
    }
    Ok(())
}

async fn forward_client_request(
    subsys: &Subsystem,
    recv: Option<ProxyMessage>,
    leader_tx: &mut OwnedWriteHalf,
    timeout: Option<Duration>,
    leader_addr: String,
) -> ControlFlow<()> {
    let Some(request) = recv else {
        return ControlFlow::Break(());
    };

    debug!("Forwarding client request to leader: {request:?}");

    if let Err(e) =
        write_line_and_flush(|| subsys.shutdown_requested(), request, leader_tx, timeout).await
    {
        error!(
            "Failed to forward client request to leader {}: {e}",
            leader_addr
        );
        return ControlFlow::Break(());
    }

    trace!("Client request forwarded to leader.");

    ControlFlow::Continue(())
}
