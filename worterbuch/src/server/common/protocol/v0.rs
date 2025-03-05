use crate::{
    Config, PStateAggregator,
    auth::{JwtClaims, get_claims},
    server::common::{CloneableWbApi, SubscriptionInfo},
    subscribers::SubscriptionId,
};
use serde_json::json;
use std::time::Duration;
use tokio::{spawn, sync::mpsc};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;
use worterbuch_common::{
    Ack, AuthorizationRequest, ClientMessage as CM, Delete, Err, ErrorCode, Get, Ls, LsState,
    PDelete, PGet, PLs, PState, PStateEvent, PSubscribe, Privilege, Publish, SPub, SPubInit,
    ServerMessage, Set, State, StateEvent, Subscribe, SubscribeLs, TransactionId, Unsubscribe,
    UnsubscribeLs,
    error::{Context, WorterbuchError, WorterbuchResult},
};

#[derive(Clone)]
pub struct V0 {
    pub client_id: Uuid,
    pub tx: mpsc::Sender<ServerMessage>,
    pub auth_required: bool,
    pub config: Config,
    pub worterbuch: CloneableWbApi,
}

impl V0 {
    pub async fn process_incoming_message(
        &self,
        msg: CM,
        authorized: &mut Option<JwtClaims>,
    ) -> WorterbuchResult<()> {
        match msg {
            CM::AuthorizationRequest(msg) => {
                if authorized.is_some() {
                    return Err(WorterbuchError::AlreadyAuthorized);
                }
                trace!("Authorizing client {} …", self.client_id);
                *authorized = Some(self.authorize(msg).await?);
                trace!("Authorizing client {} done.", self.client_id);
            }
            CM::Get(msg) => {
                if self
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Getting value for client {} …", self.client_id);
                    self.get(msg).await?;
                    trace!("Getting value for client {} done.", self.client_id);
                }
            }
            CM::PGet(msg) => {
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
                    self.pget(msg).await?;
                    trace!("PGetting values for client {} done.", self.client_id);
                }
            }
            CM::Set(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Setting value for client {} …", self.client_id);
                    self.set(msg).await?;
                    trace!("Setting value for client {} done.", self.client_id);
                }
            }
            CM::SPubInit(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!(
                        "Mapping key to transaction ID for for client {} …",
                        self.client_id
                    );
                    self.spub_init(msg).await?;
                    trace!(
                        "Mapping key to transaction ID for client {} done.",
                        self.client_id
                    );
                }
            }
            CM::SPub(msg) => {
                trace!("Setting value for client {} …", self.client_id);
                self.spub(msg).await?;
                trace!("Setting value for client {} done.", self.client_id);
            }
            CM::Publish(msg) => {
                if self
                    .check_auth(Privilege::Write, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Publishing value for client {} …", self.client_id);
                    self.publish(msg).await?;
                    trace!("Publishing value for client {} done.", self.client_id);
                }
            }
            CM::Subscribe(msg) => {
                if self
                    .check_auth(Privilege::Read, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Making subscription for client {} …", self.client_id);
                    self.subscribe(msg).await?;
                    trace!("Making subscription for client {} done.", self.client_id);
                }
            }
            CM::PSubscribe(msg) => {
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
                    self.psubscribe(msg).await?;
                    trace!("Making psubscription for client {} done.", self.client_id);
                }
            }
            CM::Unsubscribe(msg) => self.unsubscribe(msg).await?,
            CM::Delete(msg) => {
                if self
                    .check_auth(Privilege::Delete, &msg.key, authorized, msg.transaction_id)
                    .await?
                {
                    trace!("Deleting value for client {} …", self.client_id);
                    self.delete(msg).await?;
                    trace!("Deleting value for client {} done.", self.client_id);
                }
            }
            CM::PDelete(msg) => {
                if self
                    .check_auth(
                        Privilege::Delete,
                        &msg.request_pattern,
                        authorized,
                        msg.transaction_id,
                    )
                    .await?
                {
                    trace!("DPeleting value for client {} …", self.client_id);
                    self.pdelete(msg).await?;
                    trace!("DPeleting value for client {} done.", self.client_id);
                }
            }
            CM::Ls(msg) => {
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
                    self.ls(msg).await?;
                    trace!("Listing subkeys for client {} done.", self.client_id);
                }
            }
            CM::PLs(msg) => {
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
                    self.pls(msg).await?;
                    trace!(
                        "Listing matching subkeys for client {} done.",
                        self.client_id
                    );
                }
            }
            CM::SubscribeLs(msg) => {
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
                    self.subscribe_ls(msg).await?;
                    trace!("Subscribing to subkeys for client {} done.", self.client_id);
                }
            }
            CM::UnsubscribeLs(msg) => {
                trace!("Unsubscribing from subkeys for client {} …", self.client_id);
                self.unsubscribe_ls(msg).await?;
                trace!(
                    "Unsubscribing from subkeys for client {} done.",
                    self.client_id
                );
            }

            CM::ProtocolSwitchRequest(_) | CM::CGet(_) | CM::CSet(_) | CM::Transform(_) => {
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
                    if let Err(e) = claims.authorize(&privilege, pattern) {
                        trace!("Client is not authorized, sending error …");
                        self.handle_store_error(
                            WorterbuchError::Unauthorized(e.clone()),
                            transaction_id,
                        )
                        .await?;
                        trace!("Client is not authorized, sending error done.");
                        return Ok(false);
                    }
                }
                None => return Err(WorterbuchError::AuthorizationRequired(privilege)),
            }
        }
        Ok(true)
    }

    pub async fn handle_store_error(
        &self,
        e: WorterbuchError,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let error_code = ErrorCode::from(&e);
        let err_msg = format!("{e}");
        let err = Err {
            error_code,
            transaction_id,
            metadata: json!(err_msg).to_string(),
        };
        trace!("Error in store, queuing error message for client …");
        let res = self
            .tx
            .send(ServerMessage::Err(err))
            .await
            .context(|| "Error sending ERR message to client".to_owned());
        trace!("Error in store, queuing error message for client done");
        res
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
                self.handle_store_error(WorterbuchError::Unauthorized(e.clone()), 0)
                    .await?;
                Err(WorterbuchError::Unauthorized(e))
            }
        }
    }

    pub async fn get(&self, msg: Get) -> WorterbuchResult<()> {
        let value = match self.worterbuch.get(msg.key).await {
            Ok(it) => it,
            Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
        };

        let response = State {
            transaction_id: msg.transaction_id,
            event: StateEvent::Value(value),
        };

        self.tx
            .send(ServerMessage::State(response))
            .await
            .context(|| {
                format!(
                    "Error sending STATE message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn pget(&self, msg: PGet) -> WorterbuchResult<()> {
        let values = match self.worterbuch.pget(msg.request_pattern.clone()).await {
            Ok(values) => values.into_iter().collect(),
            Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
        };

        let response = PState {
            transaction_id: msg.transaction_id,
            request_pattern: msg.request_pattern,
            event: PStateEvent::KeyValuePairs(values),
        };

        self.tx
            .send(ServerMessage::PState(response))
            .await
            .context(|| {
                format!(
                    "Error sending PSTATE message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn set(&self, msg: Set) -> WorterbuchResult<()> {
        if let Err(e) = self
            .worterbuch
            .set(msg.key, msg.value, self.client_id)
            .await
        {
            self.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Value set, queuing Ack …");
        let res = self.tx.send(ServerMessage::Ack(response)).await;
        trace!("Value set, queuing Ack done.");
        res.context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

        Ok(())
    }

    pub async fn spub_init(&self, msg: SPubInit) -> WorterbuchResult<()> {
        if let Err(e) = self
            .worterbuch
            .spub_init(msg.transaction_id, msg.key, self.client_id)
            .await
        {
            self.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Value set, queuing Ack …");
        let res = self.tx.send(ServerMessage::Ack(response)).await;
        trace!("Value set, queuing Ack done.");
        res.context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

        Ok(())
    }

    pub async fn spub(&self, msg: SPub) -> WorterbuchResult<()> {
        if let Err(e) = self
            .worterbuch
            .spub(msg.transaction_id, msg.value, self.client_id)
            .await
        {
            self.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        trace!("Value set, queuing Ack …");
        let res = self.tx.send(ServerMessage::Ack(response)).await;
        trace!("Value set, queuing Ack done.");
        res.context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

        Ok(())
    }

    pub async fn publish(&self, msg: Publish) -> WorterbuchResult<()> {
        if let Err(e) = self.worterbuch.publish(msg.key, msg.value).await {
            self.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        self.tx
            .send(ServerMessage::Ack(response))
            .await
            .context(|| {
                format!(
                    "Error sending ACK message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn subscribe(&self, msg: Subscribe) -> WorterbuchResult<bool> {
        let (mut rx, subscription) = match self
            .worterbuch
            .subscribe(
                self.client_id,
                msg.transaction_id,
                msg.key.clone(),
                msg.unique,
                msg.live_only.unwrap_or(false),
            )
            .await
        {
            Ok(it) => it,
            Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(false);
            }
        };

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        self.tx
            .send(ServerMessage::Ack(response))
            .await
            .context(|| {
                format!(
                    "Error sending ACK message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        let transaction_id = msg.transaction_id;

        let wb_unsub = self.worterbuch.clone();
        let client_sub = self.tx.clone();
        let client_id = self.client_id;
        spawn(async move {
            debug!("Receiving events for subscription {subscription:?} …");
            while let Some(event) = rx.recv().await {
                let state = State {
                    transaction_id,
                    event,
                };
                if let Err(e) = client_sub.send(ServerMessage::State(state)).await {
                    error!("Error sending STATE message to client: {e}");
                    break;
                };
            }

            match wb_unsub.unsubscribe(client_id, transaction_id).await {
                Ok(()) => {
                    warn!("Subscription was not cleaned up properly!");
                }
                Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                Err(e) => {
                    warn!("Error while unsubscribing: {e}");
                }
            }
        });

        Ok(true)
    }

    pub async fn psubscribe(&self, msg: PSubscribe) -> WorterbuchResult<bool> {
        let live_only = msg.live_only.unwrap_or(false);

        let (rx, subscription) = match self
            .worterbuch
            .psubscribe(
                self.client_id,
                msg.transaction_id,
                msg.request_pattern.clone(),
                msg.unique,
                live_only,
            )
            .await
        {
            Ok(rx) => rx,
            Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(false);
            }
        };

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        self.tx
            .send(ServerMessage::Ack(response))
            .await
            .context(|| {
                format!(
                    "Error sending ACK message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        let transaction_id = msg.transaction_id;
        let request_pattern = msg.request_pattern;

        let wb_unsub = self.worterbuch.clone();
        let client_sub = self.tx.clone();
        let client_id = self.client_id;

        let channel_buffer_size = self.worterbuch.config().await?.channel_buffer_size;

        let aggregate_events = msg.aggregate_events.map(Duration::from_millis);
        if let Some(aggregate_duration) = aggregate_events {
            let subscription = SubscriptionInfo {
                aggregate_duration,
                channel_buffer_size,
                live_only,
                request_pattern,
                transaction_id,
            };
            spawn(async move {
                aggregate_loop(rx, subscription, client_sub, client_id).await;

                match wb_unsub.unsubscribe(client_id, transaction_id).await {
                    Ok(()) => {
                        warn!("Subscription was not cleaned up properly!");
                    }
                    Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                    Err(e) => {
                        warn!("Error while unsubscribing: {e}");
                    }
                }
            });
        } else {
            spawn(async move {
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
                        warn!("Error while unsubscribing: {e}");
                    }
                }
            });
        }

        Ok(true)
    }

    pub async fn unsubscribe(&self, msg: Unsubscribe) -> WorterbuchResult<()> {
        if let Err(e) = self
            .worterbuch
            .unsubscribe(self.client_id, msg.transaction_id)
            .await
        {
            self.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        };
        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        self.tx
            .send(ServerMessage::Ack(response))
            .await
            .context(|| {
                format!(
                    "Error sending ACK message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn delete(&self, msg: Delete) -> WorterbuchResult<()> {
        let value = match self.worterbuch.delete(msg.key, self.client_id).await {
            Ok(it) => it,
            Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
        };

        let response = State {
            transaction_id: msg.transaction_id,
            event: StateEvent::Deleted(value),
        };

        self.tx
            .send(ServerMessage::State(response))
            .await
            .context(|| {
                format!(
                    "Error sending STATE message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn pdelete(&self, msg: PDelete) -> WorterbuchResult<()> {
        let deleted = match self
            .worterbuch
            .pdelete(msg.request_pattern.clone(), self.client_id)
            .await
        {
            Ok(it) => it,
            Result::Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
        };

        let response = PState {
            transaction_id: msg.transaction_id,
            request_pattern: msg.request_pattern,
            event: PStateEvent::Deleted(if msg.quiet.unwrap_or(false) {
                vec![]
            } else {
                deleted
            }),
        };

        self.tx
            .send(ServerMessage::PState(response))
            .await
            .context(|| {
                format!(
                    "Error sending PSTATE message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn ls(&self, msg: Ls) -> WorterbuchResult<()> {
        let children = match self.worterbuch.ls(msg.parent).await {
            Ok(it) => it,
            Result::Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
        };

        let response = LsState {
            transaction_id: msg.transaction_id,
            children,
        };

        self.tx
            .send(ServerMessage::LsState(response))
            .await
            .context(|| {
                format!(
                    "Error sending LSSTATE message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn pls(&self, msg: PLs) -> WorterbuchResult<()> {
        let children = match self.worterbuch.pls(msg.parent_pattern).await {
            Ok(it) => it,
            Result::Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(());
            }
        };

        let response = LsState {
            transaction_id: msg.transaction_id,
            children,
        };

        self.tx
            .send(ServerMessage::LsState(response))
            .await
            .context(|| {
                format!(
                    "Error sending LSSTATE message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }

    pub async fn subscribe_ls(&self, msg: SubscribeLs) -> WorterbuchResult<bool> {
        let (mut rx, subscription) = match self
            .worterbuch
            .subscribe_ls(self.client_id, msg.transaction_id, msg.parent.clone())
            .await
        {
            Ok(it) => it,
            Err(e) => {
                self.handle_store_error(e, msg.transaction_id).await?;
                return Ok(false);
            }
        };

        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        self.tx
            .send(ServerMessage::Ack(response))
            .await
            .context(|| {
                format!(
                    "Error sending ACK message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        let transaction_id = msg.transaction_id;

        let wb_unsub = self.worterbuch.clone();
        let client_sub = self.tx.clone();
        let client_id = self.client_id;

        spawn(async move {
            debug!("Receiving events for ls subscription {subscription:?} …");
            while let Some(children) = rx.recv().await {
                let state = LsState {
                    transaction_id,
                    children,
                };
                if let Err(e) = client_sub.send(ServerMessage::LsState(state)).await {
                    error!("Error sending STATE message to client: {e}");
                    break;
                };
            }

            match wb_unsub.unsubscribe_ls(client_id, transaction_id).await {
                Ok(()) => {
                    warn!("Ls Subscription was not cleaned up properly!");
                }
                Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                Err(e) => {
                    warn!("Error while unsubscribing ls: {e}");
                }
            }
        });

        Ok(true)
    }

    pub async fn unsubscribe_ls(&self, msg: UnsubscribeLs) -> WorterbuchResult<()> {
        if let Err(e) = self
            .worterbuch
            .unsubscribe_ls(self.client_id, msg.transaction_id)
            .await
        {
            self.handle_store_error(e, msg.transaction_id).await?;
            return Ok(());
        }
        let response = Ack {
            transaction_id: msg.transaction_id,
        };

        self.tx
            .send(ServerMessage::Ack(response))
            .await
            .context(|| {
                format!(
                    "Error sending ACK message for transaction ID {}",
                    msg.transaction_id
                )
            })?;

        Ok(())
    }
}

async fn forward_loop(
    mut rx: mpsc::Receiver<PStateEvent>,
    transaction_id: TransactionId,
    request_pattern: String,
    subscription: SubscriptionId,
    client_sub: mpsc::Sender<ServerMessage>,
) {
    debug!("Receiving events for subscription {subscription:?} …");
    while let Some(event) = rx.recv().await {
        let event = PState {
            transaction_id,
            request_pattern: request_pattern.clone(),
            event,
        };
        if let Err(e) = client_sub.send(ServerMessage::PState(event)).await {
            error!("Error sending STATE message to client: {e}");
            break;
        }
    }
}

async fn aggregate_loop(
    mut rx: mpsc::Receiver<PStateEvent>,
    subscription: SubscriptionInfo,
    client_sub: mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) {
    if !subscription.live_only {
        debug!("Immediately forwarding current state to new subscription {subscription:?} …");

        if let Some(event) = rx.recv().await {
            let event = PState {
                transaction_id: subscription.transaction_id,
                request_pattern: subscription.request_pattern.clone(),
                event,
            };

            if let Err(e) = client_sub.send(ServerMessage::PState(event)).await {
                error!("Error sending STATE message to client: {e}");
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

    while let Some(event) = rx.recv().await {
        if let Err(e) = aggregator.aggregate(event).await {
            error!("Error sending STATE message to client: {e}");
            break;
        }
    }
}
