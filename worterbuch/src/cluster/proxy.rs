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
            LeaderMessage, LeaderWelcome, Locks, ProxyHandshake, ProxyMessage, Request, StateSync,
        },
        shutdown,
    },
    error::{WorterbuchAppError, WorterbuchAppResult},
    persistence::unlock_persistence,
    server::common::WbFunction,
    worterbuch::Worterbuch,
    worterbuch_version,
};
use hashbrown::HashMap;
use serde_json::json;
use std::{
    net::{SocketAddr, ToSocketAddrs},
    ops::ControlFlow,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::{mpsc, oneshot},
};
use tosub::SubsystemHandle;
use totils::while_select;
use tracing::{debug, error, info, trace, warn};
use worterbuch_common::{
    ClientId, INTERNAL_CLIENT_ID,
    error::{ConfigError, ConnectionResult, WorterbuchResult},
    is_grave_goods_topic, is_last_will_topic,
    protocol::v1::{
        CSet, ClientMessage, Delete, InternalAction, KeyValuePairs, Lock, PDelete, PStateEvent,
        ProtocolSwitchRequest, Publish, SPub, SPubInit, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER,
        SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT, ServerMessage, Set, StateEvent, Trace, TransactionId,
        Value,
    },
    receive_msg, topic, write_line_and_flush,
};

pub(crate) async fn run(
    subsys: &SubsystemHandle,
    mut worterbuch: Worterbuch,
    mut api_rx: mpsc::Receiver<WbFunction>,
    config: Config,
    servers: Servers,
    leader_addresses: Vec<String>,
) -> WorterbuchAppResult<()> {
    #[cfg(feature = "commercial")]
    if !config.license.features.proxy {
        return Err(crate::error::WorterbuchAppError::NoLicense(
            "proxy".to_owned(),
        ));
    }

    let leader_addresses = leader_addresses
        .iter()
        .map(ToSocketAddrs::to_socket_addrs)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            WorterbuchAppError::ConfigError(ConfigError::InvalidLeaderAddress(e, leader_addresses))
        })?
        .into_iter()
        .flatten()
        .collect::<Vec<SocketAddr>>();

    info!("Running in PROXY mode. Leaders: {:?}", leader_addresses);

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Proxy),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::Startup),
            true,
        )
        .await?;

    // TODO get from config
    let retry_seconds = 1;

    let mut counter = 0;

    let mut locks = Locks::default();

    'outer: loop {
        for leader_address in &leader_addresses {
            worterbuch
                .internal_set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
                    json!(&LeaderState::Disconnected),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::LeaderSync),
                    true,
                )
                .await?;

            if counter >= leader_addresses.len() {
                warn!("Could not connect to any leader. Retrying in {retry_seconds} second(s) …");
                select! {
                    biased;
                    _ = subsys.shutdown_requested() => break 'outer,
                    _ = tokio::time::sleep(Duration::from_secs(retry_seconds)) => counter = 0,
                }
            }

            select! {
                biased;
                _ = subsys.shutdown_requested() => break 'outer,
                res = run_with_leader(
                    subsys,
                    &mut worterbuch,
                    &mut api_rx,
                    config.clone(),
                    *leader_address,
                    &mut locks,
                ) => {
                    let initial_connection_successful =  res?;
                    if initial_connection_successful {
                        info!("Connection to leader {} lost. Trying next leader …", leader_address);
                        counter = 1;
                    } else {
                        info!("Could not connect to leader {}. Trying next leader …", leader_address);
                        counter += 1;
                    }
                },
            }
        }
    }

    info!("Main loop stopped, shutting down.");

    shutdown(subsys, worterbuch, config, servers).await
}

async fn run_with_leader(
    subsys: &SubsystemHandle,
    worterbuch: &mut Worterbuch,
    api_rx: &mut mpsc::Receiver<WbFunction>,
    config: Config,
    leader_address: SocketAddr,
    locks: &mut Locks,
) -> WorterbuchAppResult<bool> {
    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
            json!(LeaderState::Connecting(leader_address)),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::LeaderSync),
            true,
        )
        .await?;

    let stream = match TcpStream::connect(leader_address).await {
        Ok(it) => it,
        Err(e) => {
            warn!("Failed to connect to leader {}: {}", leader_address, e);
            return Ok(false);
        }
    };
    let (leader_rx, leader_tx) = stream.into_split();
    let mut lines = BufReader::new(leader_rx).lines();

    let proxy_request_tx = init_request_sender(subsys, leader_tx, &config, leader_address);

    let timeout = config.initial_sync_timeout;

    info!("Successfully connected to leader {leader_address}. Performing handshake …");

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
            json!(LeaderState::Handshake(leader_address)),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::LeaderSync),
            true,
        )
        .await?;

    let welcome = select! {
        biased;
        _ = subsys.shutdown_requested() => {
            warn!("Shutdown requested before receiving leader welcome message.");
            return Err(WorterbuchAppError::ClusterError("shut down before receiving leader welcome message".to_owned()));
        },
        recv = receive_msg(&mut lines, timeout) => {
            debug!("Received leader message");
            match recv {
                Ok(Some(msg)) => {
                    if let LeaderMessage::Welcome(welcome) = msg {
                        debug!("Received welcome message from leader: {welcome:?}");
                        welcome
                    } else {
                        warn!("Expected welcome message from leader, but got: {msg:?}");
                        return Ok(false);
                    }
                },
                Ok(None) => {
                    warn!("Leader closed connection before sending welcome message.");
                    return Ok(false);
                },
                Err(e) => {
                    warn!("Error receiving welcome message from leader: {e}");
                    return Ok(false);
                }
            }
        },
    };

    // TODO check version
    send_handshake(welcome, worterbuch, &config, &proxy_request_tx, locks).await?;

    info!("Handshake complete. Waiting for initial sync message …");

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
            json!(LeaderState::Syncing(leader_address)),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::LeaderSync),
            true,
        )
        .await?;

    let mut persistence_interval = config.persistence_interval();

    select! {
        biased;
        _ = subsys.shutdown_requested() => {
            warn!("Shutdown requested before initial sync completed.");
            return Err(WorterbuchAppError::ClusterError("shut down before initial sync".to_owned()));
        },
        recv = receive_msg(&mut lines, timeout) => {
            debug!("Received leader message");
            match recv {
                Ok(Some(msg)) => {
                    if let LeaderMessage::Init(state) = msg {
                        debug!("Received initial sync message from leader: {state:?}");
                        initial_sync(state, worterbuch).await?;
                        persistence_interval.reset();
                        worterbuch.flush().await?;
                    } else {
                        warn!("Expected initial sync message from leader, but got: {msg:?}");
                        return Ok(false);
                    }
                },
                Ok(None) => {
                    warn!("Leader closed connection before sending initial sync message.");
                    return Ok(false);
                },
                Err(e) => {
                    warn!("Error receiving initial sync message from leader: {e}");
                    return Ok(false);
                }
            }
        },
    }
    info!("Successfully synced with leader.");

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLUSTER, SYSTEM_TOPIC_LEADER),
            json!(LeaderState::Synced(leader_address)),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::LeaderSync),
            true,
        )
        .await?;

    LeaderConnection::new(subsys, proxy_request_tx, worterbuch, api_rx, lines)
        .run()
        .await?;

    info!(
        "Proxy loop for leader {} stopped, closing connection.",
        leader_address
    );

    Ok(true)
}

async fn send_handshake(
    welcome: LeaderWelcome,
    worterbuch: &Worterbuch,
    config: &Config,
    proxy_request_tx: &mpsc::Sender<ProxyMessage>,
    locks: &Locks,
) -> WorterbuchAppResult<()> {
    let version = worterbuch_version();
    let auth_token = if welcome.authentication_required {
        todo!()
    } else {
        None
    };

    let connected_clients = connected_clients(worterbuch);

    let handshake = ProxyMessage::Handshake(Handshake::Proxy(ProxyHandshake {
        version,
        auth_token,
        locks: locks.clone(),
        connected_clients,
    }));

    proxy_request_tx.send(handshake).await?;

    Ok(())
}

#[derive(Debug, Default)]
struct ClientResponseInterests {
    ack: HashMap<TransactionId, oneshot::Sender<WorterbuchResult<()>>>,
    state: HashMap<TransactionId, oneshot::Sender<WorterbuchResult<Value>>>,
    pstate: HashMap<TransactionId, oneshot::Sender<WorterbuchResult<KeyValuePairs>>>,
    lock_acquired: HashMap<TransactionId, oneshot::Sender<WorterbuchResult<oneshot::Receiver<()>>>>,
}
impl ClientResponseInterests {
    fn is_empty(&self) -> bool {
        self.state.is_empty()
            && self.pstate.is_empty()
            && self.lock_acquired.is_empty()
            && self.ack.is_empty()
    }
}

struct LeaderConnection<'a> {
    subsys: &'a SubsystemHandle,
    response_interests: HashMap<ClientId, ClientResponseInterests>,
    proxy_request_tx: mpsc::Sender<ProxyMessage>,
    worterbuch: &'a mut Worterbuch,
    api_rx: &'a mut mpsc::Receiver<WbFunction>,
    lines: Lines<BufReader<OwnedReadHalf>>,
}

impl<'a> LeaderConnection<'a> {
    fn new(
        subsys: &'a SubsystemHandle,
        proxy_request_tx: mpsc::Sender<ProxyMessage>,
        worterbuch: &'a mut Worterbuch,
        api_rx: &'a mut mpsc::Receiver<WbFunction>,
        lines: Lines<BufReader<OwnedReadHalf>>,
    ) -> Self {
        Self {
            subsys,
            response_interests: HashMap::new(),
            proxy_request_tx,
            worterbuch,
            api_rx,
            lines,
        }
    }

    async fn run(mut self) -> WorterbuchAppResult<()> {
        while_select! {
            biased;
            _ = self.subsys.shutdown_requested() => break,
            recv = receive_msg(&mut self.lines, None) => self.try_process_leader_message(recv).await?,
            recv = self.api_rx.recv() => self.try_process_api_call(recv).await?,
        }
        Ok(())
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
            WbFunction::Connected(client_id, addr, protocol, tx) => {
                let request = ProxyMessage::Connected(Connected {
                    client_id,
                    protocol: protocol.clone(),
                });
                self.proxy_request_tx.send(request).await?;
                cluster::process_api_call(
                    self.worterbuch,
                    WbFunction::Connected(client_id, addr, protocol, tx),
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
            }
            WbFunction::ProtocolSwitched(client_id, interface, version) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::ProtocolSwitchRequest(ProtocolSwitchRequest { version }),
                    interface: interface.clone(),
                });
                // TODO register response interest
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
                    let request = ProxyMessage::Request(Request {
                        client_id,
                        msg: ClientMessage::Set(Set {
                            transaction_id,
                            key,
                            value,
                        }),
                        interface,
                    });
                    self.register_ack_interest(client_id, transaction_id, tx);
                    self.proxy_request_tx.send(request).await?;
                }
            }
            WbFunction::CSet(transaction_id, interface, key, value, version, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::CSet(CSet {
                        transaction_id,
                        key,
                        value,
                        version,
                    }),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::SPubInit(transaction_id, interface, key, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::SPubInit(SPubInit {
                        transaction_id,
                        key,
                    }),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::SPub(transaction_id, interface, value, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::SPub(SPub {
                        transaction_id,
                        value,
                    }),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::Publish(transaction_id, interface, key, value, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::Publish(Publish {
                        transaction_id,
                        key,
                        value,
                    }),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::Delete(transaction_id, interface, key, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::Delete(Delete {
                        transaction_id,
                        key,
                    }),
                    interface,
                });
                self.register_state_interest(client_id, transaction_id, tx);
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
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::PDelete(PDelete {
                        transaction_id,
                        request_pattern,
                        quiet,
                    }),
                    interface,
                });
                self.register_pstate_interest(client_id, transaction_id, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::Lock(transaction_id, interface, key, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::Lock(Lock {
                        transaction_id,
                        key,
                    }),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::AcquireLock(transaction_id, interface, key, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::AcquireLock(Lock {
                        transaction_id,
                        key,
                    }),
                    interface,
                });
                self.register_lock_acquired_interest(client_id, transaction_id, tx);
                self.proxy_request_tx.send(request).await?;
            }
            WbFunction::ReleaseLock(transaction_id, interface, key, client_id, tx) => {
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: ClientMessage::ReleaseLock(Lock {
                        transaction_id,
                        key,
                    }),
                    interface,
                });
                self.register_ack_interest(client_id, transaction_id, tx);
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

    fn register_ack_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        tx: oneshot::Sender<WorterbuchResult<()>>,
    ) {
        self.response_interests
            .entry(client_id)
            .or_default()
            .ack
            .insert(transaction_id, tx);
    }

    fn register_state_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        tx: oneshot::Sender<WorterbuchResult<Value>>,
    ) {
        self.response_interests
            .entry(client_id)
            .or_default()
            .state
            .insert(transaction_id, tx);
    }

    fn register_pstate_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        tx: oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ) {
        self.response_interests
            .entry(client_id)
            .or_default()
            .pstate
            .insert(transaction_id, tx);
    }

    fn register_lock_acquired_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        tx: oneshot::Sender<WorterbuchResult<oneshot::Receiver<()>>>,
    ) {
        self.response_interests
            .entry(client_id)
            .or_default()
            .lock_acquired
            .insert(transaction_id, tx);
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
                if let Some(tx) = self.get_ack_response_interest(client_id, e.transaction_id) {
                    tx.send(Err(e.into())).ok();
                } else if let Some(tx) =
                    self.get_state_response_interest(client_id, e.transaction_id)
                {
                    tx.send(Err(e.into())).ok();
                } else if let Some(tx) =
                    self.get_pstate_response_interest(client_id, e.transaction_id)
                {
                    tx.send(Err(e.into())).ok();
                }
            }
            ServerMessage::LockLost(_) => {
                warn!("Received unexpected LockLost message from leader");
            }
        }

        Ok(())
    }

    fn get_ack_response_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<oneshot::Sender<WorterbuchResult<()>>> {
        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests.ack.remove(&transaction_id);
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

        tx
    }

    fn get_state_response_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<oneshot::Sender<WorterbuchResult<Value>>> {
        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests.state.remove(&transaction_id);
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

        tx
    }

    fn get_pstate_response_interest(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<oneshot::Sender<WorterbuchResult<KeyValuePairs>>> {
        let interests = self.response_interests.get_mut(&client_id)?;
        let tx = interests.pstate.remove(&transaction_id);
        if interests.is_empty() {
            self.response_interests.remove(&client_id);
        }

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
    subsys: &SubsystemHandle,
    leader_tx: OwnedWriteHalf,
    config: &Config,
    leader_addr: SocketAddr,
) -> mpsc::Sender<ProxyMessage> {
    let (tx, rx) = mpsc::channel(config.channel_buffer_size);
    let send_timeout = config.send_timeout;
    subsys.spawn("proxy_request_sender", move |s| {
        request_sender_loop(s, leader_tx, rx, send_timeout, leader_addr)
    });
    tx
}

async fn request_sender_loop(
    subsys: SubsystemHandle,
    mut leader_tx: OwnedWriteHalf,
    mut rx: mpsc::Receiver<ProxyMessage>,
    timeout: Option<Duration>,
    leader_addr: SocketAddr,
) -> miette::Result<()> {
    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = rx.recv() => forward_client_request(&subsys, recv, &mut leader_tx, timeout, leader_addr).await,
    }
    Ok(())
}

async fn forward_client_request(
    subsys: &SubsystemHandle,
    recv: Option<ProxyMessage>,
    leader_tx: &mut OwnedWriteHalf,
    timeout: Option<Duration>,
    leader_addr: SocketAddr,
) -> ControlFlow<()> {
    let Some(request) = recv else {
        return ControlFlow::Break(());
    };

    debug!("Forwarding client request to leader: {request:?}");

    if let Err(e) = write_line_and_flush(
        || subsys.shutdown_requested(),
        request,
        leader_tx,
        timeout,
        leader_addr,
    )
    .await
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

async fn initial_sync(
    state_sync: StateSync,
    worterbuch: &mut Worterbuch,
) -> WorterbuchAppResult<()> {
    worterbuch
        .reset_store_and_notify_subscribers(state_sync.store, true)
        .await?;

    worterbuch
        .internal_set(
            topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_MODE),
            json!(Mode::Proxy),
            INTERNAL_CLIENT_ID,
            Trace::InternalAction(InternalAction::LeaderSync),
            true,
        )
        .await?;

    unlock_persistence();

    worterbuch.flush().await.map_err(|e| {
        WorterbuchAppError::ClusterError(format!("Failed to flush storage after initial sync: {e}"))
    })?;
    Ok(())
}
