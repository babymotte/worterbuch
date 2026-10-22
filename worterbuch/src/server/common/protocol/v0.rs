/*
 *  Worterbuch client protocol v0 implementation
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
    Config,
    auth::{JwtClaims, get_claims},
    server::common::{CloneableWbApi, SubscriptionInfo, protocol::ServerMessageBroadcaster},
    worterbuch::PStateAggregator,
};
use serde_json::json;
use std::time::Duration;
use tosub::Subsystem;
use tracing::{Level, debug, instrument, trace, warn};
use worterbuch_common::{
    AuthCheck, ClientId, PSubscriptionReceiver, Privilege, SubscriptionId, WbApi,
    error::{Context, WorterbuchError, WorterbuchResult},
    protocol::v1::{
        Ack, AuthorizationRequest, ClientInfo, ClientMessage, Delete, Err, ErrorCode, Get, Ls,
        LsState, PDelete, PGet, PLs, PState, PStateEvent, PSubscribe, Publish, SPub, SPubInit,
        ServerMessage, Set, State, StateEvent, Subscribe, SubscribeLs, TransactionId, Unsubscribe,
        UnsubscribeLs,
    },
};

#[derive(Clone)]
pub struct V0 {
    pub subsys: Subsystem,
    pub client_info: ClientInfo,
    pub tx: ServerMessageBroadcaster,
    pub auth_required: bool,
    pub config: Config,
    pub worterbuch: CloneableWbApi,
}

impl V0 {
    pub fn client_id(&self) -> ClientId {
        self.client_info.client_id
    }

    pub fn client_info(&self) -> ClientInfo {
        self.client_info.clone()
    }

    #[instrument(level=Level::TRACE, skip(self), fields(protocol = "v0", client_id=%self.client_info.client_id))]
    pub async fn process_incoming_message(
        &self,
        msg: ClientMessage,
        authorized: &mut Option<JwtClaims>,
    ) -> WorterbuchResult<()> {
        match msg {
            ClientMessage::AuthorizationRequest(msg) => {
                if authorized.is_some() {
                    return Err(WorterbuchError::AlreadyAuthorized);
                }
                trace!("Authorizing client {} …", self.client_id());
                *authorized = Some(self.authorize(msg).await?);
                trace!("Authorizing client {} done.", self.client_id());
            }
            ClientMessage::Get(msg) => {
                if self
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Getting value for client {} …", self.client_id());
                    self.get(msg).await;
                    trace!("Getting value for client {} done.", self.client_id());
                }
            }
            ClientMessage::PGet(msg) => {
                if self
                    .check_auth(
                        Privilege::Read,
                        &msg.request_pattern,
                        authorized,
                        msg.transaction_id,
                    )
                    .await?
                {
                    trace!("PGetting values for client {} …", self.client_id());
                    self.pget(msg).await;
                    trace!("PGetting values for client {} done.", self.client_id());
                }
            }
            ClientMessage::Set(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Setting value for client {} …", self.client_id());
                    self.set(msg).await;
                    trace!("Setting value for client {} done.", self.client_id());
                }
            }
            ClientMessage::SPubInit(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!(
                        "Mapping key to transaction ID for for client {} …",
                        self.client_id()
                    );
                    self.spub_init(msg).await;
                    trace!(
                        "Mapping key to transaction ID for client {} done.",
                        self.client_id()
                    );
                }
            }
            ClientMessage::SPub(msg) => {
                trace!("Setting value for client {} …", self.client_id());
                self.spub(msg).await;
                trace!("Setting value for client {} done.", self.client_id());
            }
            ClientMessage::Publish(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Publishing value for client {} …", self.client_id());
                    self.publish(msg).await;
                    trace!("Publishing value for client {} done.", self.client_id());
                }
            }
            ClientMessage::Subscribe(msg) => {
                if self
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Making subscription for client {} …", self.client_id());
                    self.subscribe(msg).await;
                    trace!("Making subscription for client {} done.", self.client_id());
                }
            }
            ClientMessage::PSubscribe(msg) => {
                if self
                    .check_auth(
                        Privilege::Read,
                        &msg.request_pattern,
                        authorized,
                        msg.transaction_id,
                    )
                    .await?
                {
                    trace!("Making psubscription for client {} …", self.client_id());
                    self.psubscribe(msg).await;
                    trace!("Making psubscription for client {} done.", self.client_id());
                }
            }
            ClientMessage::Unsubscribe(msg) => self.unsubscribe(msg).await,
            ClientMessage::Delete(msg) => {
                if self
                    .check_auth(Privilege::Delete, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Deleting value for client {} …", self.client_id());
                    self.delete(msg).await;
                    trace!("Deleting value for client {} done.", self.client_id());
                }
            }
            ClientMessage::PDelete(msg) => {
                if self
                    .check_auth(
                        Privilege::Delete,
                        &msg.request_pattern,
                        authorized,
                        msg.transaction_id,
                    )
                    .await?
                {
                    trace!("PDeleting value for client {} …", self.client_id());
                    self.pdelete(msg).await;
                    trace!("PDeleting value for client {} done.", self.client_id());
                }
            }
            ClientMessage::Ls(msg) => {
                let pattern = &msg
                    .parent
                    .as_ref()
                    .map(|it| format!("{it}/?"))
                    .unwrap_or("?".to_owned());
                if self
                    .check_auth(Privilege::Read, pattern, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Listing subkeys for client {} …", self.client_id());
                    self.ls(msg).await;
                    trace!("Listing subkeys for client {} done.", self.client_id());
                }
            }
            ClientMessage::PLs(msg) => {
                let pattern = &msg
                    .parent_pattern
                    .as_ref()
                    .map(|it| format!("{it}/?"))
                    .unwrap_or("?".to_owned());
                if self
                    .check_auth(Privilege::Read, pattern, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Listing matching subkeys for client {} …", self.client_id());
                    self.pls(msg).await;
                    trace!(
                        "Listing matching subkeys for client {} done.",
                        self.client_id()
                    );
                }
            }
            ClientMessage::SubscribeLs(msg) => {
                let pattern = &msg
                    .parent
                    .as_ref()
                    .map(|it| format!("{it}/?"))
                    .unwrap_or("?".to_owned());
                if self
                    .check_auth(Privilege::Read, pattern, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Subscribing to subkeys for client {} …", self.client_id());
                    self.subscribe_ls(msg).await;
                    trace!("Subscribing to subkeys for client {} done.", self.client_id());
                }
            }
            ClientMessage::UnsubscribeLs(msg) => {
                trace!("Unsubscribing from subkeys for client {} …", self.client_id());
                self.unsubscribe_ls(msg).await;
                trace!(
                    "Unsubscribing from subkeys for client {} done.",
                    self.client_id()
                );
            }

            ClientMessage::ProtocolSwitchRequest(_)
            | ClientMessage::CGet(_)
            | ClientMessage::CSet(_)
            // | ClientMessage::Transform(_)
            | ClientMessage::Lock(_)
            | ClientMessage::AcquireLock(_)
            | ClientMessage::ReleaseLock(_) => {
                return Err(WorterbuchError::NotImplemented);
            }
        };
        Ok(())
    }

    pub async fn check_auth(
        &self,
        privilege: Privilege,
        pattern: &str,
        authorized: &Option<JwtClaims>,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<bool> {
        if self.auth_required {
            match authorized {
                Some(claims) => {
                    if let Err(e) = claims.authorize(&privilege, AuthCheck::Pattern(pattern)) {
                        trace!("Client is not authorized, sending error …");
                        handle_store_error(
                            &self.tx,
                            WorterbuchError::Unauthorized(e.clone()),
                            transaction_id,
                        )
                        .await;
                        trace!("Client is not authorized, sending error done.");
                        return Ok(false);
                    }
                }
                None => return Err(WorterbuchError::AuthorizationRequired(privilege)),
            }
        }
        Ok(true)
    }

    async fn authorize(&self, msg: AuthorizationRequest) -> WorterbuchResult<JwtClaims> {
        match get_claims(Some(&msg.auth_token), &self.config) {
            Ok(claims) => {
                self.tx
                    .send(ServerMessage::Authorized(Ack { transaction_id: 0 }))
                    .await
                    .context(|| "Error sending HANDSHAKE message".to_owned())?;
                Ok(claims)
            }
            Err(e) => {
                handle_store_error(&self.tx, WorterbuchError::Unauthorized(e.clone()), 0).await;
                Err(WorterbuchError::Unauthorized(e))
            }
        }
    }

    pub async fn get(&self, msg: Get) {
        let transaction_id = msg.transaction_id;
        let value = match self.worterbuch.get(msg, self.client_info()).await {
            Ok(it) => it,
            Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return;
            }
        };

        let response = State {
            transaction_id,
            event: StateEvent::Value(value),
            trace: None,
        };

        let msg = ServerMessage::State(response);
        let _ = self.tx.send(msg).await;
    }

    pub async fn pget(&self, msg: PGet) {
        let transaction_id = msg.transaction_id;
        let request_pattern = msg.request_pattern.clone();
        let values = match self.worterbuch.pget(msg, self.client_info()).await {
            Ok(values) => values.into_iter().collect(),
            Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return;
            }
        };

        let response = PState {
            transaction_id,
            request_pattern,
            event: PStateEvent::KeyValuePairs(values),
            trace: None,
        };

        let msg = ServerMessage::PState(response);

        let _ = self.tx.send(msg).await;
    }

    #[instrument(level = Level::TRACE, skip(self), fields(client_id=%self.client_id()))]
    pub async fn set(&self, msg: Set) {
        let transaction_id = msg.transaction_id;
        if let Err(e) = self.worterbuch.set(msg, self.client_info()).await {
            handle_store_error(&self.tx, e, transaction_id).await;
            return;
        }
        let response = Ack { transaction_id };

        trace!("Value set, queuing Ack …");
        let msg = ServerMessage::Ack(response);
        let _ = self.tx.send(msg).await;
        trace!("Value set, queuing Ack done.");
    }

    pub async fn spub_init(&self, msg: SPubInit) {
        let transaction_id = msg.transaction_id;
        if let Err(e) = self.worterbuch.spub_init(msg, self.client_info()).await {
            handle_store_error(&self.tx, e, transaction_id).await;
            return;
        }

        let response = Ack { transaction_id };

        trace!("Value set, queuing Ack …");
        let msg = ServerMessage::Ack(response);
        let _ = self.tx.send(msg).await;
        trace!("Value set, queuing Ack done.");
    }

    pub async fn spub(&self, msg: SPub) {
        let transaction_id = msg.transaction_id;
        if let Err(e) = self.worterbuch.spub(msg, self.client_info()).await {
            handle_store_error(&self.tx, e, transaction_id).await;
            return;
        }

        let response = Ack { transaction_id };

        trace!("Value set, queuing Ack …");
        let msg = ServerMessage::Ack(response);
        let _ = self.tx.send(msg).await;
        trace!("Value set, queuing Ack done.");
    }

    pub async fn publish(&self, msg: Publish) {
        let transaction_id = msg.transaction_id;
        if let Err(e) = self.worterbuch.publish(msg, self.client_info()).await {
            handle_store_error(&self.tx, e, transaction_id).await;
            return;
        }

        let response = Ack { transaction_id };

        let msg = ServerMessage::Ack(response);
        let _ = self.tx.send(msg).await;
    }

    pub async fn subscribe(&self, msg: Subscribe) -> bool {
        let transaction_id = msg.transaction_id;
        let (mut rx, subscription) = match self.worterbuch.subscribe(msg, self.client_info()).await
        {
            Ok(it) => it,
            Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return false;
            }
        };

        let response = Ack {
            transaction_id: transaction_id,
        };

        let smsg = ServerMessage::Ack(response);
        let _ = self.tx.send(smsg).await;

        let wb_unsub = self.worterbuch.named("unsubscribe");
        let client_sub = self.tx.clone();
        let client_info = self.client_info();

        self.subsys
            .spawn("protocol/subscribe/send-loop", move |_| async move {
                debug!("Receiving events for subscription {subscription:?} …");
                while let Some((event, trace)) = rx.recv().await {
                    let state = State {
                        transaction_id,
                        event,
                        trace,
                    };
                    if let Err(e) = client_sub.send(ServerMessage::State(state)).await {
                        debug!("Error sending STATE message to client: {e}");
                        break;
                    };
                }

                let req = Unsubscribe { transaction_id };
                match wb_unsub.unsubscribe(req, client_info).await {
                    Ok(()) => {
                        warn!("Subscription was not cleaned up properly!");
                    }
                    Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                    Err(e) => {
                        debug!("Error while unsubscribing: {e}");
                    }
                }
            });

        true
    }

    pub async fn psubscribe(&self, msg: PSubscribe) -> bool {
        let transaction_id = msg.transaction_id;
        let live_only = msg.live_only.unwrap_or(false);
        let aggregate_events = msg.aggregate_events.map(Duration::from_millis);
        let request_pattern = msg.request_pattern.clone();

        let (rx, subscription) = match self.worterbuch.psubscribe(msg, self.client_info()).await {
            Ok(rx) => rx,
            Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return false;
            }
        };

        let response = Ack { transaction_id };

        let smsg = ServerMessage::Ack(response);
        let _ = self.tx.send(smsg).await;

        let wb_unsub = self.worterbuch.named("unsubscribe");
        let client_sub = self.tx.clone();
        let client_info = self.client_info();

        let channel_buffer_size = self.worterbuch.config().channel_buffer_size;

        if let Some(aggregate_duration) = aggregate_events {
            let subscription = SubscriptionInfo {
                aggregate_duration,
                channel_buffer_size,
                live_only,
                request_pattern,
                transaction_id,
            };
            self.subsys
                .spawn("protocol/pSubscribe/aggregate-loop", move |_| async move {
                    aggregate_loop(rx, subscription, client_sub, client_info.client_id).await;

                    let req = Unsubscribe { transaction_id };
                    match wb_unsub.unsubscribe(req, client_info).await {
                        Ok(()) => {
                            warn!("Subscription was not cleaned up properly!");
                        }
                        Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                        Err(e) => {
                            debug!("Error while unsubscribing: {e}");
                        }
                    }
                });
        } else {
            self.subsys
                .spawn("protocol/pSubscribe/send-loop", move |_| async move {
                    forward_loop(
                        rx,
                        transaction_id,
                        request_pattern,
                        subscription,
                        client_sub,
                    )
                    .await;

                    let req = Unsubscribe { transaction_id };
                    match wb_unsub.unsubscribe(req, client_info).await {
                        Ok(()) => {
                            warn!("Subscription was not cleaned up properly!");
                        }
                        Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                        Err(e) => {
                            debug!("Error while unsubscribing: {e}");
                        }
                    }
                });
        }

        true
    }

    pub async fn unsubscribe(&self, msg: Unsubscribe) {
        let transaction_id = msg.transaction_id;
        if let Err(e) = self.worterbuch.unsubscribe(msg, self.client_info()).await {
            handle_store_error(&self.tx, e, transaction_id).await;
            return;
        };
        let response = Ack { transaction_id };

        let msg = ServerMessage::Ack(response);

        let _ = self.tx.send(msg).await;
    }

    pub async fn delete(&self, msg: Delete) {
        let transaction_id = msg.transaction_id;
        let value = match self.worterbuch.delete(msg, self.client_info()).await {
            Ok(it) => it,
            Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return;
            }
        };

        let response = State {
            transaction_id,
            event: StateEvent::Deleted(value),
            trace: None,
        };

        let msg = ServerMessage::State(response);

        let _ = self.tx.send(msg).await;
    }

    pub async fn pdelete(&self, msg: PDelete) {
        let transaction_id = msg.transaction_id;
        let request_pattern = msg.request_pattern.clone();
        let quiet = msg.quiet.unwrap_or(false);
        let deleted = match self.worterbuch.pdelete(msg, self.client_info()).await {
            Ok(it) => it,
            Result::Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return;
            }
        };

        let response = PState {
            transaction_id,
            request_pattern,
            event: PStateEvent::Deleted(if quiet { vec![] } else { deleted }),
            trace: None,
        };

        let msg = ServerMessage::PState(response);
        let _ = self.tx.send(msg).await;
    }

    pub async fn ls(&self, msg: Ls) {
        let transaction_id = msg.transaction_id;
        let children = match self.worterbuch.ls(msg, self.client_info()).await {
            Ok(it) => it,
            Result::Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return;
            }
        };

        let response = LsState {
            transaction_id,
            children,
            trace: None,
        };

        let msg = ServerMessage::LsState(response);

        let _ = self.tx.send(msg).await;
    }

    pub async fn pls(&self, msg: PLs) {
        let transaction_id = msg.transaction_id;
        let children = match self.worterbuch.pls(msg, self.client_info()).await {
            Ok(it) => it,
            Result::Err(e) => {
                handle_store_error(&self.tx, e, transaction_id).await;
                return;
            }
        };

        let response = LsState {
            transaction_id,
            children,
            trace: None,
        };

        let msg = ServerMessage::LsState(response);

        let _ = self.tx.send(msg).await;
    }

    pub async fn subscribe_ls(&self, msg: SubscribeLs) -> bool {
        let transaction_id = msg.transaction_id;
        let (mut rx, subscription) =
            match self.worterbuch.subscribe_ls(msg, self.client_info()).await {
                Ok(it) => it,
                Err(e) => {
                    handle_store_error(&self.tx, e, transaction_id).await;
                    return false;
                }
            };

        let response = Ack { transaction_id };

        let smsg = ServerMessage::Ack(response);
        let _ = self.tx.send(smsg).await;

        let wb_unsub = self.worterbuch.named("unsubscribe");
        let client_sub = self.tx.clone();
        let client_info = self.client_info();

        self.subsys.spawn(
            format!("protocol/lsSubscribe/send-loop/{subscription}"),
            move |_| async move {
                debug!("Receiving events for ls subscription {subscription:?} …");
                while let Some((children, trace)) = rx.recv().await {
                    let state = LsState {
                        transaction_id,
                        children,
                        trace,
                    };
                    if let Err(e) = client_sub.send(ServerMessage::LsState(state)).await {
                        debug!("Error sending LSSTATE message to client: {e}");
                        break;
                    };
                }

                let req = UnsubscribeLs { transaction_id };
                match wb_unsub.unsubscribe_ls(req, client_info).await {
                    Ok(()) => {
                        warn!("Ls Subscription was not cleaned up properly!");
                    }
                    Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                    Err(e) => {
                        debug!("Error while unsubscribing ls: {e}");
                    }
                }
            },
        );

        true
    }

    pub async fn unsubscribe_ls(&self, msg: UnsubscribeLs) {
        let transaction_id = msg.transaction_id;
        if let Err(e) = self
            .worterbuch
            .unsubscribe_ls(msg, self.client_info())
            .await
        {
            handle_store_error(&self.tx, e, transaction_id).await;
            return;
        }
        let response = Ack { transaction_id };

        let msg = ServerMessage::Ack(response);

        let _ = self.tx.send(msg).await;
    }
}

pub async fn handle_store_error(
    tx: &ServerMessageBroadcaster,
    e: WorterbuchError,
    transaction_id: TransactionId,
) {
    let msg = err_msg(e, transaction_id);
    trace!("Error in store, queuing error message for client …");
    let _ = tx.send(msg).await;
    trace!("Error in store, queuing error message for client done");
}

fn err_msg(e: WorterbuchError, transaction_id: u64) -> ServerMessage {
    let error_code = ErrorCode::from(&e);
    let err_msg = format!("{e}");
    let err = Err {
        error_code,
        transaction_id,
        metadata: json!(err_msg).to_string(),
    };

    ServerMessage::Err(err)
}

async fn forward_loop(
    mut rx: PSubscriptionReceiver,
    transaction_id: TransactionId,
    request_pattern: String,
    subscription: SubscriptionId,
    client_sub: ServerMessageBroadcaster,
) {
    debug!("Receiving events for subscription {subscription:?} …");
    while let Some((event, trace)) = rx.recv().await {
        let event = PState {
            transaction_id,
            request_pattern: request_pattern.clone(),
            event,
            trace,
        };
        if let Err(e) = client_sub.send(ServerMessage::PState(event)).await {
            debug!("Error sending PSTATE message to client: {e}");
            break;
        }
    }
}

async fn aggregate_loop(
    mut rx: PSubscriptionReceiver,
    subscription: SubscriptionInfo,
    client_sub: ServerMessageBroadcaster,
    client_id: ClientId,
) {
    if !subscription.live_only {
        debug!("Immediately forwarding current state to new subscription {subscription:?} …");

        if let Some((event, trace)) = rx.recv().await {
            let event = PState {
                transaction_id: subscription.transaction_id,
                request_pattern: subscription.request_pattern.clone(),
                event,
                trace,
            };

            if let Err(e) = client_sub.send(ServerMessage::PState(event)).await {
                debug!("Error sending PSTATE message to client: {e}");
                return;
            }
        } else {
            return;
        }
    }

    debug!("Aggregating events for subscription {subscription:?} …");

    let aggregator = PStateAggregator::new(
        client_sub,
        subscription.request_pattern,
        subscription.aggregate_duration,
        subscription.transaction_id,
        subscription.channel_buffer_size,
        client_id,
    );

    while let Some((event, _)) = rx.recv().await {
        if let Err(e) = aggregator.aggregate(event).await {
            debug!("Error sending STATE message to client: {e}");
            break;
        }
    }
}
