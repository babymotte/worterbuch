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
    server::common::{
        CloneableWbApi, PendingResponse, SubscriptionInfo,
        protocol::{LazyBroadcaster, ServerMessageLazyBroadcaster},
    },
    worterbuch::PStateAggregator,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tosub::Subsystem;
use tracing::{Level, debug, instrument, trace, warn};
use worterbuch_common::{
    AuthCheck, ClientId, PSubscriptionReceiver, Privilege, SubscriptionId, WbApi,
    error::{Context, WorterbuchError, WorterbuchResult},
    protocol::v1::{
        Ack, AuthorizationRequest, ClientMessage, Delete, Err, ErrorCode, Get, Ls, LsState,
        PDelete, PGet, PLs, PState, PStateEvent, PSubscribe, Publish, SPub, SPubInit,
        ServerMessage, Set, State, StateEvent, Subscribe, SubscribeLs, TransactionId, Unsubscribe,
        UnsubscribeLs,
    },
};

#[derive(Clone)]
pub struct V0 {
    pub subsys: Subsystem,
    pub client_id: ClientId,
    pub tx: ServerMessageLazyBroadcaster,
    pub auth_required: bool,
    pub config: Config,
    pub worterbuch: CloneableWbApi,
    pub semaphore: Arc<Semaphore>,
}

impl V0 {
    #[instrument(level=Level::TRACE, skip(self), fields(protocol = "v0", client_id=%self.client_id))]
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
                trace!("Authorizing client {} …", self.client_id);
                *authorized = Some(self.authorize(msg).await?);
                trace!("Authorizing client {} done.", self.client_id);
            }
            ClientMessage::Get(msg) => {
                if self
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Getting value for client {} …", self.client_id);
                    self.get(msg).await;
                    trace!("Getting value for client {} done.", self.client_id);
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
                    trace!("PGetting values for client {} …", self.client_id);
                    self.pget(msg).await;
                    trace!("PGetting values for client {} done.", self.client_id);
                }
            }
            ClientMessage::Set(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Setting value for client {} …", self.client_id);
                    self.set(msg).await;
                    trace!("Setting value for client {} done.", self.client_id);
                }
            }
            ClientMessage::SPubInit(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!(
                        "Mapping key to transaction ID for for client {} …",
                        self.client_id
                    );
                    self.spub_init(msg).await;
                    trace!(
                        "Mapping key to transaction ID for client {} done.",
                        self.client_id
                    );
                }
            }
            ClientMessage::SPub(msg) => {
                trace!("Setting value for client {} …", self.client_id);
                self.spub(msg).await;
                trace!("Setting value for client {} done.", self.client_id);
            }
            ClientMessage::Publish(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Publishing value for client {} …", self.client_id);
                    self.publish(msg).await;
                    trace!("Publishing value for client {} done.", self.client_id);
                }
            }
            ClientMessage::Subscribe(msg) => {
                if self
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Making subscription for client {} …", self.client_id);
                    self.subscribe(msg).await;
                    trace!("Making subscription for client {} done.", self.client_id);
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
                    trace!("Making psubscription for client {} …", self.client_id);
                    self.psubscribe(msg).await;
                    trace!("Making psubscription for client {} done.", self.client_id);
                }
            }
            ClientMessage::Unsubscribe(msg) => self.unsubscribe(msg).await,
            ClientMessage::Delete(msg) => {
                if self
                    .check_auth(Privilege::Delete, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Deleting value for client {} …", self.client_id);
                    self.delete(msg).await;
                    trace!("Deleting value for client {} done.", self.client_id);
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
                    trace!("PDeleting value for client {} …", self.client_id);
                    self.pdelete(msg).await;
                    trace!("PDeleting value for client {} done.", self.client_id);
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
                    trace!("Listing subkeys for client {} …", self.client_id);
                    self.ls(msg).await;
                    trace!("Listing subkeys for client {} done.", self.client_id);
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
                    trace!("Listing matching subkeys for client {} …", self.client_id);
                    self.pls(msg).await;
                    trace!(
                        "Listing matching subkeys for client {} done.",
                        self.client_id
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
                    trace!("Subscribing to subkeys for client {} …", self.client_id);
                    self.subscribe_ls(msg).await;
                    trace!("Subscribing to subkeys for client {} done.", self.client_id);
                }
            }
            ClientMessage::UnsubscribeLs(msg) => {
                trace!("Unsubscribing from subkeys for client {} …", self.client_id);
                self.unsubscribe_ls(msg).await;
                trace!(
                    "Unsubscribing from subkeys for client {} done.",
                    self.client_id
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
                        handle_store_error_lazy(
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
                    .lazy_send(ServerMessage::Authorized(Ack { transaction_id: 0 }))
                    .await
                    .context(|| "Error sending HANDSHAKE message".to_owned())?;
                Ok(claims)
            }
            Err(e) => {
                handle_store_error_lazy(&self.tx, WorterbuchError::Unauthorized(e.clone()), 0)
                    .await;
                Err(WorterbuchError::Unauthorized(e))
            }
        }
    }

    pub async fn get(&self, msg: Get) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch.get_deferred(msg.key),
            move |value| {
                ServerMessage::State(State {
                    transaction_id,
                    event: StateEvent::Value(value),
                    trace: None,
                })
            },
        )
        .await;
    }

    pub async fn pget(&self, msg: PGet) {
        let transaction_id = msg.transaction_id;
        let request_pattern = msg.request_pattern.clone();
        self.respond(
            transaction_id,
            self.worterbuch.pget_deferred(msg.request_pattern),
            move |values| {
                ServerMessage::PState(PState {
                    transaction_id,
                    request_pattern,
                    event: PStateEvent::KeyValuePairs(values.into_iter().collect()),
                    trace: None,
                })
            },
        )
        .await;
    }

    #[instrument(level = Level::TRACE, skip(self), fields(client_id=%self.client_id))]
    pub async fn set(&self, msg: Set) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch
                .set_deferred(transaction_id, msg.key, msg.value, self.client_id),
            move |()| ServerMessage::Ack(Ack { transaction_id }),
        )
        .await;
    }

    pub async fn spub_init(&self, msg: SPubInit) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch
                .spub_init_deferred(transaction_id, msg.key, self.client_id),
            move |()| ServerMessage::Ack(Ack { transaction_id }),
        )
        .await;
    }

    pub async fn spub(&self, msg: SPub) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch
                .spub_deferred(transaction_id, msg.value, self.client_id),
            move |()| ServerMessage::Ack(Ack { transaction_id }),
        )
        .await;
    }

    pub async fn publish(&self, msg: Publish) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch
                .publish_deferred(transaction_id, msg.key, msg.value, self.client_id),
            move |()| ServerMessage::Ack(Ack { transaction_id }),
        )
        .await;
    }

    pub async fn subscribe(&self, msg: Subscribe) -> bool {
        let (mut rx, subscription) = match self
            .worterbuch
            .subscribe(
                self.client_id,
                msg.transaction_id,
                msg.key.clone(),
                msg.unique.unwrap_or(false),
                msg.live_only.unwrap_or(false),
                msg.send_traces.unwrap_or(false),
            )
            .await
        {
            Ok(it) => it,
            Err(e) => {
                handle_store_error_lazy(&self.tx, e, msg.transaction_id).await;
                return false;
            }
        };

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        let smsg = ServerMessage::Ack(response);
        let _ = self.tx.lazy_send(smsg).await;

        let transaction_id = msg.transaction_id;

        let wb_unsub = self.worterbuch.named("unsubscribe");
        let client_sub = self.tx.clone();
        let client_id = self.client_id;

        self.subsys
            .spawn("protocol/subscribe/send-loop", move |_| async move {
                debug!("Receiving events for subscription {subscription:?} …");
                while let Some((event, trace)) = rx.recv().await {
                    let state = State {
                        transaction_id,
                        event,
                        trace,
                    };
                    if let Err(e) = client_sub.lazy_send(ServerMessage::State(state)).await {
                        debug!("Error sending STATE message to client: {e}");
                        break;
                    };
                }

                match wb_unsub.unsubscribe(client_id, transaction_id).await {
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
        let live_only = msg.live_only.unwrap_or(false);

        let (rx, subscription) = match self
            .worterbuch
            .psubscribe(
                self.client_id,
                msg.transaction_id,
                msg.request_pattern.clone(),
                msg.unique.unwrap_or(false),
                live_only,
                msg.send_traces.unwrap_or(false),
            )
            .await
        {
            Ok(rx) => rx,
            Err(e) => {
                handle_store_error_lazy(&self.tx, e, msg.transaction_id).await;
                return false;
            }
        };

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        let smsg = ServerMessage::Ack(response);
        let _ = self.tx.lazy_send(smsg).await;

        let transaction_id = msg.transaction_id;
        let request_pattern = msg.request_pattern;

        let wb_unsub = self.worterbuch.named("unsubscribe");
        let client_sub = self.tx.clone();
        let client_id = self.client_id;

        let channel_buffer_size = self.worterbuch.config().channel_buffer_size;

        let aggregate_events = msg.aggregate_events.map(Duration::from_millis);
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
                    aggregate_loop(rx, subscription, client_sub, client_id).await;

                    match wb_unsub.unsubscribe(client_id, transaction_id).await {
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

                    match wb_unsub.unsubscribe(client_id, transaction_id).await {
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
        self.respond(
            transaction_id,
            self.worterbuch
                .unsubscribe_deferred(self.client_id, transaction_id),
            move |()| ServerMessage::Ack(Ack { transaction_id }),
        )
        .await;
    }

    pub async fn delete(&self, msg: Delete) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch
                .delete_deferred(transaction_id, msg.key, self.client_id),
            move |value| {
                ServerMessage::State(State {
                    transaction_id,
                    event: StateEvent::Deleted(value),
                    trace: None,
                })
            },
        )
        .await;
    }

    pub async fn pdelete(&self, msg: PDelete) {
        let transaction_id = msg.transaction_id;
        let request_pattern = msg.request_pattern.clone();
        let quiet = msg.quiet.unwrap_or(false);
        self.respond(
            transaction_id,
            self.worterbuch.pdelete_deferred(
                transaction_id,
                msg.request_pattern,
                msg.quiet,
                self.client_id,
            ),
            move |deleted| {
                ServerMessage::PState(PState {
                    transaction_id,
                    request_pattern,
                    event: PStateEvent::Deleted(if quiet { vec![] } else { deleted }),
                    trace: None,
                })
            },
        )
        .await;
    }

    pub async fn ls(&self, msg: Ls) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch.ls_deferred(msg.parent),
            move |children| {
                ServerMessage::LsState(LsState {
                    transaction_id,
                    children,
                    trace: None,
                })
            },
        )
        .await;
    }

    pub async fn pls(&self, msg: PLs) {
        let transaction_id = msg.transaction_id;
        self.respond(
            transaction_id,
            self.worterbuch.pls_deferred(msg.parent_pattern),
            move |children| {
                ServerMessage::LsState(LsState {
                    transaction_id,
                    children,
                    trace: None,
                })
            },
        )
        .await;
    }

    pub async fn subscribe_ls(&self, msg: SubscribeLs) -> bool {
        let (mut rx, subscription) = match self
            .worterbuch
            .subscribe_ls(
                self.client_id,
                msg.transaction_id,
                msg.parent.clone(),
                msg.send_traces.unwrap_or(false),
            )
            .await
        {
            Ok(it) => it,
            Err(e) => {
                handle_store_error_lazy(&self.tx, e, msg.transaction_id).await;
                return false;
            }
        };

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        let smsg = ServerMessage::Ack(response);
        let _ = self.tx.lazy_send(smsg).await;

        let transaction_id = msg.transaction_id;

        let wb_unsub = self.worterbuch.named("unsubscribe");
        let client_sub = self.tx.clone();
        let client_id = self.client_id;

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
                    if let Err(e) = client_sub.lazy_send(ServerMessage::LsState(state)).await {
                        debug!("Error sending LSSTATE message to client: {e}");
                        break;
                    };
                }

                match wb_unsub.unsubscribe_ls(client_id, transaction_id).await {
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
        self.respond(
            transaction_id,
            self.worterbuch
                .unsubscribe_ls_deferred(self.client_id, transaction_id),
            move |()| ServerMessage::Ack(Ack { transaction_id }),
        )
        .await;
    }

    pub(crate) async fn acquire_permit(&self) -> OwnedSemaphorePermit {
        self.semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("Semaphore acquire failed")
    }

    /// Sends a request to the core system and queues the response for the client. The request has been handed to the
    /// core system by the time this returns, which guarantees that a client's requests are processed in the order in
    /// which the client sent them. Only waiting for the response happens concurrently.
    pub(crate) async fn respond<T: Send + 'static>(
        &self,
        transaction_id: TransactionId,
        request: impl Future<Output = WorterbuchResult<PendingResponse<T>>>,
        to_message: impl FnOnce(T) -> ServerMessage + Send + 'static,
    ) {
        let permit = self.acquire_permit().await;
        let response = request.await;
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            let result = match response {
                Ok(response) => response.response().await,
                Err(e) => Err(e),
            };
            let msg = match result {
                Ok(value) => to_message(value),
                Err(e) => {
                    trace!("Error in store, queuing error message for client.");
                    err_msg(e, transaction_id)
                }
            };
            let _ = tx.send(msg);
            drop(permit);
        });

        let _ = self.tx.send(rx).await;
    }
}

pub async fn handle_store_error_lazy(
    tx: &ServerMessageLazyBroadcaster,
    e: WorterbuchError,
    transaction_id: TransactionId,
) {
    let msg = err_msg(e, transaction_id);
    trace!("Error in store, queuing error message for client …");
    let _ = tx.lazy_send(msg).await;
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
    let msg = ServerMessage::Err(err);
    msg
}

async fn forward_loop(
    mut rx: PSubscriptionReceiver,
    transaction_id: TransactionId,
    request_pattern: String,
    subscription: SubscriptionId,
    client_sub: ServerMessageLazyBroadcaster,
) {
    debug!("Receiving events for subscription {subscription:?} …");
    while let Some((event, trace)) = rx.recv().await {
        let event = PState {
            transaction_id,
            request_pattern: request_pattern.clone(),
            event,
            trace,
        };
        if let Err(e) = client_sub.lazy_send(ServerMessage::PState(event)).await {
            debug!("Error sending PSTATE message to client: {e}");
            break;
        }
    }
}

async fn aggregate_loop(
    mut rx: PSubscriptionReceiver,
    subscription: SubscriptionInfo,
    client_sub: ServerMessageLazyBroadcaster,
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

            if let Err(e) = client_sub.lazy_send(ServerMessage::PState(event)).await {
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
