/*
 *  Helper functions for proxy mode
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
    cluster::{
        self,
        protocol::{
            ClientWriteCommand, ClusterStateChange, Connected, Disconnected, LeaderMessage,
            ProxyMessage, Request, locks::Locks,
        },
        proxy::{ClientResponseInterests, read_stdin, update_leader_addresses},
    },
    server::common::WbFunction,
    worterbuch::Worterbuch,
};
use hashbrown::HashMap;
use miette::{Context, IntoDiagnostic, bail};
use std::ops::ControlFlow;
use tokio::{
    io::{BufReader, Lines},
    net::tcp::OwnedReadHalf,
    select, spawn,
    sync::{mpsc, oneshot},
};
use tosub::Subsystem;
use tracing::{debug, error, trace, warn};
use worterbuch_common::{
    ClientId, INTERNAL_CLIENT_ID, LockLostSender,
    error::{ConnectionResult, WorterbuchResult},
    is_grave_goods_topic, is_last_will_topic,
    protocol::v1::{
        CSet, ClientMessage, Delete, ErrorCode, Key, KeyValuePairs, Lock, PDelete, PStateEvent,
        ProtocolSwitchRequest, Publish, SPub, SPubInit, ServerMessage, Set, StateEvent,
        TransactionId, Value,
    },
    receive_msg,
};

pub struct LeaderConnection<'a> {
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
    pub fn new(
        subsys: &'a Subsystem,
        proxy_request_tx: mpsc::Sender<ProxyMessage>,
        worterbuch: &'a mut Worterbuch,
        api_rx: &'a mut mpsc::Receiver<WbFunction>,
        lines: Lines<BufReader<OwnedReadHalf>>,
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

    pub async fn run(mut self) -> miette::Result<bool> {
        debug!(
            "Starting new leder session with inherited response interests: {:#?}",
            self.response_interests
        );

        loop {
            trace!("Entering select loop for leader connection");
            let control_flow = select! {
                _ = self.subsys.shutdown_requested() => break,
                recv = read_stdin(self.stdin, self.read_leader_addresses_from_stdin, self.subsys.shutdown_requested()) => self.update_leader_address(recv),
                recv = receive_msg(&mut self.lines, None) => self.try_process_leader_message(recv).await?,
                recv = self.api_rx.recv() => self.try_process_api_call(recv).await?,
            };
            match control_flow {
                ControlFlow::Continue(()) => {
                    trace!("Continuing select loop for leader connection");
                    continue;
                }
                ControlFlow::Break(()) => {
                    trace!("Breaking select loop for leader connection");
                    break;
                }
            }
        }

        let leader_addresses_updated = self.leader_addresses_updated;

        Ok(leader_addresses_updated)
    }

    fn update_leader_address(&mut self, recv: Option<String>) -> ControlFlow<()> {
        trace!(enter = "update_leader_address");
        if *self.read_leader_addresses_from_stdin
            && update_leader_addresses(
                recv,
                self.leader_addresses,
                *self.read_leader_addresses_from_stdin,
            )
        {
            self.leader_addresses_updated = true;
            if self.leader_addresses.contains(&self.leader_address) {
                trace!(exit = "update_leader_address");
                ControlFlow::Continue(())
            } else {
                warn!(
                    "Current leader address {} is no longer in the list of known leader addresses {:?}",
                    self.leader_address, self.leader_addresses
                );
                trace!(exit = "update_leader_address");
                ControlFlow::Break(())
            }
        } else {
            trace!(exit = "update_leader_address");
            ControlFlow::Continue(())
        }
    }

    async fn try_process_leader_message(
        &mut self,
        recv: ConnectionResult<Option<LeaderMessage>>,
    ) -> miette::Result<ControlFlow<()>> {
        trace!(enter = "try_process_leader_message");
        match recv {
            Ok(Some(msg)) => {
                self.process_leader_message(msg)
                    .await
                    .wrap_err("failed to process leader message")?;
                trace!(exit = "try_process_leader_message");
                Ok(ControlFlow::Continue(()))
            }
            Ok(None) => {
                trace!(exit = "try_process_leader_message");
                Ok(ControlFlow::Break(()))
            }
            Err(e) => {
                error!("Error receiving update from leader: {e}");
                trace!(exit = "try_process_leader_message");
                Ok(ControlFlow::Break(()))
            }
        }
    }

    async fn process_leader_message(&mut self, msg: LeaderMessage) -> miette::Result<()> {
        trace!(enter = "process_leader_message");
        debug!("Processing leader sync message: {msg:?}");

        let res = match msg {
            LeaderMessage::Welcome(_) => {
                trace!(exit = "process_leader_message");
                bail!("already received welcome message");
            }
            LeaderMessage::Init(_) => {
                trace!(exit = "process_leader_message");
                bail!("already synced");
            }
            LeaderMessage::Mut(ClusterStateChange { command, trace, .. }) => match command {
                ClientWriteCommand::Set(key, value) => {
                    self.worterbuch
                        .internal_set(
                            key,
                            value,
                            trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                            trace,
                            true,
                        )
                        .await
                }
                ClientWriteCommand::CSet(key, value, versions) => {
                    self.worterbuch
                        .internal_cset(
                            key,
                            value,
                            versions,
                            trace.client_id().unwrap_or(INTERNAL_CLIENT_ID),
                            trace,
                            true,
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

        trace!(exit = "process_leader_message");

        Ok(())
    }

    async fn try_process_api_call(
        &mut self,
        recv: Option<WbFunction>,
    ) -> miette::Result<ControlFlow<()>> {
        trace!(enter = "try_process_api_call");
        match recv {
            Some(function) => {
                self.process_api_call(function)
                    .await
                    .wrap_err("failed to process api call")?;
                trace!(exit = "try_process_api_call");
                Ok(ControlFlow::Continue(()))
            }
            None => {
                trace!(exit = "try_process_api_call");
                Ok(ControlFlow::Break(()))
            }
        }
    }

    async fn process_api_call(&mut self, function: WbFunction) -> miette::Result<()> {
        trace!(enter = "process_api_call");
        debug!("Processing API call: {function:?}");
        match function {
            WbFunction::Connected(client_id, addr, protocol, eject, tx) => {
                let request = ProxyMessage::Connected(Connected {
                    client_id,
                    protocol: protocol.clone(),
                });
                self.queue_leader_request(request).await?;
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
                let res = self
                    .queue_leader_request(request)
                    .await
                    .wrap_err("failed to forward client disconnect to leader");
                cluster::process_api_call(
                    self.worterbuch,
                    WbFunction::Disconnected(client_id, protocol, socket_addr),
                )
                .await;
                self.locks.client_disconnected(client_id);
                self.drop_response_interests(client_id);
                res?;
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
                self.queue_leader_request(request).await?;
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
                    self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
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
                self.queue_leader_request(request).await?;
                tx.send(Ok((acked_rx, lost_rx))).ok();
            }
            WbFunction::ReleaseLock(transaction_id, interface, key, client_id, tx) => {
                let client_message = ClientMessage::ReleaseLock(Lock {
                    transaction_id,
                    key: key.clone(),
                });
                let request = ProxyMessage::Request(Request {
                    client_id,
                    msg: client_message.clone(),
                    interface,
                });
                let Some(tids) = self.locks.released(client_id, key, &self.worterbuch) else {
                    return Ok(());
                };

                for transaction_id in tids {
                    let _ = self.get_lock_acquired_interest(client_id, transaction_id);
                    let _ = self.get_lock_lost_interest(client_id, transaction_id);
                }

                self.register_ack_interest(client_id, transaction_id, client_message, tx);
                self.queue_leader_request(request)
                    .await
                    .wrap_err("failed to queue leader request")?;
            }
            WbFunction::Import(_, _, _, _, _) => {
                warn!("Import not yet implemented");
                // TODO forward to leader
                // TODO register response interest
            }
            function => cluster::process_api_call(self.worterbuch, function).await,
        };

        trace!(exit = "process_leader_message");

        Ok(())
    }

    async fn queue_leader_request(&mut self, request: ProxyMessage) -> miette::Result<()> {
        trace!(enter = "queue_leader_request");
        self.proxy_request_tx
            .send(request)
            .await
            .into_diagnostic()
            .wrap_err("failed to send request")?;
        trace!(exit = "queue_leader_request");
        Ok(())
    }

    async fn forward_leader_response(
        &mut self,
        client_id: ClientId,
        server_message: ServerMessage,
    ) -> WorterbuchResult<()> {
        match server_message {
            ServerMessage::Welcome(welcome) => {
                warn!("Received unexpected welcome message from leader");
                trace!(msg = ?welcome);
            }
            ServerMessage::CState(cstate) => {
                warn!("Received unexpected CState message from leader");
                trace!(msg = ?cstate);
            }

            ServerMessage::LsState(ls_state) => {
                warn!("Received unexpected LsState message from leader");
                trace!(msg = ?ls_state);
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
                    self.locks
                        .acquired(client_id, ack.transaction_id, &self.worterbuch);
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
                    warn!("Received unexpected StateEvent::Value message from leader");
                    trace!(msg = ?value);
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
                    warn!("Received unexpected PState::KeyValuePairs message from leader");
                    trace!(msg = ?kvps);
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
                debug!("Received error message from leader for client {client_id}: {e:?}");
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
                    self.locks
                        .acquisition_failed(client_id, transaction_id, &self.worterbuch);
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
