use crate::{subscribers::SubscriptionId, Config, PStateAggregator};
use serde::Serialize;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    spawn,
    sync::{
        mpsc::{self, UnboundedReceiver},
        oneshot,
    },
};
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    Ack, AuthToken, AuthenticationRequest, ClientMessage as CM, Delete, Err, ErrorCode, Get, Key,
    KeyValuePair, KeyValuePairs, LiveOnlyFlag, Ls, LsState, MetaData, PDelete, PGet, PState,
    PStateEvent, PSubscribe, Protocol, ProtocolVersion, Publish, RegularKeySegment, RequestPattern,
    ServerMessage, Set, State, StateEvent, Subscribe, SubscribeLs, TransactionId, UniqueFlag,
    Unsubscribe, UnsubscribeLs, Value,
};

pub async fn process_incoming_message(
    client_id: Uuid,
    msg: &str,
    worterbuch: &CloneableWbApi,
    tx: &mpsc::Sender<ServerMessage>,
    authentication_required: bool,
    already_authenticated: bool,
) -> WorterbuchResult<(bool, bool)> {
    log::debug!("Received message: {msg}");
    let mut authenticated = false;
    match serde_json::from_str(msg) {
        Ok(Some(msg)) => match msg {
            CM::AuthenticationRequest(msg) => {
                if already_authenticated {
                    return Err(WorterbuchError::AlreadyAuthenticated);
                }
                authenticate(msg, worterbuch, tx, client_id).await?;
                authenticated = true;
            }
            CM::Get(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("Get"));
                }
                get(msg, worterbuch, tx).await?;
            }
            CM::PGet(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("PGet"));
                }
                pget(msg, worterbuch, tx).await?;
            }
            CM::Set(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("Set"));
                }
                set(msg, worterbuch, tx).await?;
            }
            CM::Publish(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("Publish"));
                }
                publish(msg, worterbuch, tx).await?;
            }
            CM::Subscribe(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("Subscribe"));
                }
                subscribe(msg, client_id, worterbuch, tx).await?;
            }
            CM::PSubscribe(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("PSubscribe"));
                }
                psubscribe(msg, client_id, worterbuch, tx).await?;
            }
            CM::Unsubscribe(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("Unsubscribe"));
                }
                unsubscribe(msg, worterbuch, tx, client_id).await?
            }
            CM::Delete(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("Delete"));
                }
                delete(msg, worterbuch, tx).await?;
            }
            CM::PDelete(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("PDelete"));
                }
                pdelete(msg, worterbuch, tx).await?;
            }
            CM::Ls(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("Ls"));
                }
                ls(msg, worterbuch, tx).await?;
            }
            CM::SubscribeLs(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("SubscribeLs"));
                }
                subscribe_ls(msg, client_id, worterbuch, tx).await?;
            }
            CM::UnsubscribeLs(msg) => {
                if authentication_required && !already_authenticated {
                    return Err(WorterbuchError::AuthenticationRequired("UnsubscribeLs"));
                }
                unsubscribe_ls(msg, client_id, worterbuch, tx).await?;
            }
            CM::Keepalive => (),
        },
        Ok(None) => {
            // client disconnected
            return Ok((false, authenticated));
        }
        Err(e) => {
            log::error!("Error decoding message: {e}");
            return Ok((false, authenticated));
        }
    }

    Ok((true, authenticated))
}

pub enum WbFunction {
    Authenticate(
        Option<AuthToken>,
        Option<String>,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    Get(Key, oneshot::Sender<WorterbuchResult<(String, Value)>>),
    Set(Key, Value, oneshot::Sender<WorterbuchResult<()>>),
    Publish(Key, Value, oneshot::Sender<WorterbuchResult<()>>),
    Ls(
        Option<Key>,
        oneshot::Sender<WorterbuchResult<Vec<RegularKeySegment>>>,
    ),
    PGet(
        RequestPattern,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Subscribe(
        Uuid,
        TransactionId,
        Key,
        UniqueFlag,
        LiveOnlyFlag,
        oneshot::Sender<WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)>>,
    ),
    PSubscribe(
        Uuid,
        TransactionId,
        RequestPattern,
        UniqueFlag,
        LiveOnlyFlag,
        oneshot::Sender<WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)>>,
    ),
    SubscribeLs(
        Uuid,
        TransactionId,
        Option<Key>,
        oneshot::Sender<
            WorterbuchResult<(UnboundedReceiver<Vec<RegularKeySegment>>, SubscriptionId)>,
        >,
    ),
    Unsubscribe(Uuid, TransactionId, oneshot::Sender<WorterbuchResult<()>>),
    UnsubscribeLs(Uuid, TransactionId, oneshot::Sender<WorterbuchResult<()>>),
    Delete(Key, oneshot::Sender<WorterbuchResult<(Key, Value)>>),
    PDelete(
        RequestPattern,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Connected(Uuid, SocketAddr, Protocol),
    Disconnected(Uuid, SocketAddr),
    Config(oneshot::Sender<Config>),
    Export(oneshot::Sender<WorterbuchResult<Value>>),
    Len(oneshot::Sender<usize>),
    SupportedProtocolVersion(oneshot::Sender<ProtocolVersion>),
}

#[derive(Clone)]
pub struct CloneableWbApi {
    tx: mpsc::Sender<WbFunction>,
}

impl CloneableWbApi {
    pub fn new(tx: mpsc::Sender<WbFunction>) -> Self {
        CloneableWbApi { tx }
    }

    pub async fn authenticate(
        &self,
        auth_token: Option<String>,
        client_id: Option<String>,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Authenticate(auth_token, client_id, tx))
            .await?;
        rx.await?
    }

    pub async fn get(&self, key: Key) -> WorterbuchResult<(String, Value)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Get(key, tx)).await?;
        rx.await?
    }

    pub async fn pget<'a>(&self, pattern: RequestPattern) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::PGet(pattern, tx)).await?;
        rx.await?
    }

    pub async fn set(&self, key: Key, value: Value) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Set(key, value, tx)).await?;
        rx.await?
    }

    pub async fn publish(&self, key: Key, value: Value) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Publish(key, value, tx)).await?;
        rx.await?
    }

    pub async fn ls(&self, parent: Option<Key>) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Ls(parent, tx)).await?;
        rx.await?
    }

    pub async fn subscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Subscribe(
                client_id,
                transaction_id,
                key,
                unique,
                live_only,
                tx,
            ))
            .await?;
        rx.await?
    }

    pub async fn psubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::PSubscribe(
                client_id,
                transaction_id,
                pattern,
                unique,
                live_only,
                tx,
            ))
            .await?;
        rx.await?
    }

    pub async fn subscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        parent: Option<Key>,
    ) -> WorterbuchResult<(UnboundedReceiver<Vec<RegularKeySegment>>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::SubscribeLs(
                client_id,
                transaction_id,
                parent,
                tx,
            ))
            .await?;
        rx.await?
    }

    pub async fn unsubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Unsubscribe(client_id, transaction_id, tx))
            .await?;
        rx.await?
    }

    pub async fn unsubscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::UnsubscribeLs(client_id, transaction_id, tx))
            .await?;
        rx.await?
    }

    pub async fn delete(&self, key: Key) -> WorterbuchResult<(Key, Value)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Delete(key, tx)).await?;
        rx.await?
    }

    pub async fn pdelete(&self, pattern: RequestPattern) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::PDelete(pattern, tx)).await?;
        rx.await?
    }

    pub async fn connected(
        &self,
        client_id: Uuid,
        remote_addr: SocketAddr,
        protocol: Protocol,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::Connected(client_id, remote_addr, protocol))
            .await?;
        Ok(())
    }

    pub async fn disconnected(
        &self,
        client_id: Uuid,
        remote_addr: SocketAddr,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::Disconnected(client_id, remote_addr))
            .await?;
        Ok(())
    }

    pub async fn config(&self) -> WorterbuchResult<Config> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Config(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn export(&self) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Export(tx)).await?;
        rx.await?
    }

    pub async fn len(&self) -> WorterbuchResult<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Len(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn supported_protocol_version(&self) -> WorterbuchResult<ProtocolVersion> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::SupportedProtocolVersion(tx))
            .await?;
        Ok(rx.await?)
    }
}

async fn authenticate(
    msg: AuthenticationRequest,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    let response = match worterbuch
        .authenticate(Some(msg.auth_token), Some(client_id.to_string()))
        .await
    {
        Ok(()) => Ack { transaction_id: 0 },
        Err(e) => {
            handle_store_error(e, client, 0).await?;
            return Err(WorterbuchError::AuthenticationFailed);
        }
    };

    client
        .send(ServerMessage::Authenticated(response))
        .await
        .context(|| format!("Error sending HANDSHAKE message",))?;

    Ok(())
}

async fn get(
    msg: Get,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let key_value = match worterbuch.get(msg.key).await {
        Ok(key_value) => key_value.into(),
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = State {
        transaction_id: msg.transaction_id,
        event: StateEvent::KeyValue(key_value),
    };

    client
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

async fn pget(
    msg: PGet,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let values = match worterbuch.pget(msg.request_pattern.clone()).await {
        Ok(values) => values.into_iter().map(KeyValuePair::from).collect(),
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = PState {
        transaction_id: msg.transaction_id,
        request_pattern: msg.request_pattern,
        event: PStateEvent::KeyValuePairs(values),
    };

    client
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

async fn set(
    msg: Set,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch.set(msg.key, msg.value).await {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    client
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

async fn publish(
    msg: Publish,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch.publish(msg.key, msg.value).await {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    client
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

async fn subscribe(
    msg: Subscribe,
    client_id: Uuid,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<bool> {
    let (mut rx, subscription) = match worterbuch
        .subscribe(
            client_id,
            msg.transaction_id,
            msg.key.clone(),
            msg.unique,
            msg.live_only.unwrap_or(false),
        )
        .await
    {
        Ok(it) => it,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(false);
        }
    };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    client
        .send(ServerMessage::Ack(response))
        .await
        .context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

    let transaction_id = msg.transaction_id;

    let wb_unsub = worterbuch.clone();
    let client_sub = client.clone();

    spawn(async move {
        log::debug!("Receiving events for subscription {subscription:?} …");
        while let Some(event) = rx.recv().await {
            let state_events: Vec<StateEvent> = event.into();

            for event in state_events {
                let state = State {
                    transaction_id: transaction_id.clone(),
                    event,
                };
                if let Err(e) = client_sub.send(ServerMessage::State(state)).await {
                    log::error!("Error sending STATE message to client: {e}");
                    break;
                };
            }
        }

        match wb_unsub.unsubscribe(client_id, transaction_id).await {
            Ok(()) => {
                log::warn!("Subscription was not cleaned up properly!");
            }
            Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
            Err(e) => {
                log::warn!("Error while unsubscribing: {e}");
            }
        }
    });

    Ok(true)
}

async fn psubscribe(
    msg: PSubscribe,
    client_id: Uuid,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<bool> {
    let (rx, subscription) = match worterbuch
        .psubscribe(
            client_id,
            msg.transaction_id,
            msg.request_pattern.clone(),
            msg.unique,
            msg.live_only.unwrap_or(false),
        )
        .await
    {
        Ok(rx) => rx,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(false);
        }
    };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    client
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

    let wb_unsub = worterbuch.clone();
    let client_sub = client.clone();

    let aggregate_events = msg.aggregate_events.map(Duration::from_millis);
    if let Some(aggregate_duration) = aggregate_events {
        spawn(aggregate_psub_events(
            rx,
            transaction_id,
            request_pattern,
            client_sub,
            wb_unsub,
            client_id,
            subscription,
            aggregate_duration,
        ));
    } else {
        spawn(forward_psub_events(
            rx,
            transaction_id,
            request_pattern,
            client_sub,
            wb_unsub,
            client_id,
            subscription,
        ));
    }

    Ok(true)
}

async fn forward_psub_events(
    mut rx: mpsc::UnboundedReceiver<PStateEvent>,
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    client_sub: mpsc::Sender<ServerMessage>,
    wb_unsub: CloneableWbApi,
    client_id: Uuid,
    subscription: SubscriptionId,
) {
    log::debug!("Receiving events for subscription {subscription:?} …");
    while let Some(event) = rx.recv().await {
        let event = PState {
            transaction_id: transaction_id.clone(),
            request_pattern: request_pattern.clone(),
            event,
        };
        if let Err(e) = client_sub.send(ServerMessage::PState(event)).await {
            log::error!("Error sending STATE message to client: {e}");
            break;
        }
    }

    match wb_unsub.unsubscribe(client_id, transaction_id).await {
        Ok(()) => {
            log::warn!("Subscription was not cleaned up properly!");
        }
        Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
        Err(e) => {
            log::warn!("Error while unsubscribing: {e}");
        }
    }
}

async fn aggregate_psub_events(
    mut rx: mpsc::UnboundedReceiver<PStateEvent>,
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    client_sub: mpsc::Sender<ServerMessage>,
    wb_unsub: CloneableWbApi,
    client_id: Uuid,
    subscription: SubscriptionId,
    aggregate_duration: Duration,
) {
    log::debug!("Aggregating events for subscription {subscription:?} …");

    let aggregator = PStateAggregator::new(
        client_sub,
        request_pattern,
        aggregate_duration,
        transaction_id,
    );

    while let Some(event) = rx.recv().await {
        if let Err(e) = aggregator.aggregate(event).await {
            log::error!("Error sending STATE message to client: {e}");
            break;
        }
    }

    match wb_unsub.unsubscribe(client_id, transaction_id).await {
        Ok(()) => {
            log::warn!("Subscription was not cleaned up properly!");
        }
        Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
        Err(e) => {
            log::warn!("Error while unsubscribing: {e}");
        }
    }
}

async fn unsubscribe(
    msg: Unsubscribe,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch.unsubscribe(client_id, msg.transaction_id).await {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    };
    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    client
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

async fn delete(
    msg: Delete,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let key_value = match worterbuch.delete(msg.key).await {
        Ok(key_value) => key_value.into(),
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = State {
        transaction_id: msg.transaction_id,
        event: StateEvent::Deleted(key_value),
    };

    client
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

async fn pdelete(
    msg: PDelete,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let deleted = match worterbuch.pdelete(msg.request_pattern.clone()).await {
        Ok(it) => it,
        Result::Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = PState {
        transaction_id: msg.transaction_id,
        request_pattern: msg.request_pattern,
        event: PStateEvent::Deleted(deleted),
    };

    client
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

async fn ls(
    msg: Ls,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let children = match worterbuch.ls(msg.parent).await {
        Ok(it) => it,
        Result::Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = LsState {
        transaction_id: msg.transaction_id,
        children,
    };

    client
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

async fn subscribe_ls(
    msg: SubscribeLs,
    client_id: Uuid,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<bool> {
    let (mut rx, subscription) = match worterbuch
        .subscribe_ls(client_id, msg.transaction_id, msg.parent.clone())
        .await
    {
        Ok(it) => it,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(false);
        }
    };

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    client
        .send(ServerMessage::Ack(response))
        .await
        .context(|| {
            format!(
                "Error sending ACK message for transaction ID {}",
                msg.transaction_id
            )
        })?;

    let transaction_id = msg.transaction_id;

    let wb_unsub = worterbuch.clone();
    let client_sub = client.clone();

    spawn(async move {
        log::debug!("Receiving events for ls subscription {subscription:?} …");
        while let Some(children) = rx.recv().await {
            let state = LsState {
                transaction_id: transaction_id.clone(),
                children,
            };
            if let Err(e) = client_sub.send(ServerMessage::LsState(state)).await {
                log::error!("Error sending STATE message to client: {e}");
                break;
            };
        }

        match wb_unsub.unsubscribe_ls(client_id, transaction_id).await {
            Ok(()) => {
                log::warn!("Ls Subscription was not cleaned up properly!");
            }
            Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
            Err(e) => {
                log::warn!("Error while unsubscribing ls: {e}");
            }
        }
    });

    Ok(true)
}

async fn unsubscribe_ls(
    msg: UnsubscribeLs,
    client_id: Uuid,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch
        .unsubscribe_ls(client_id, msg.transaction_id)
        .await
    {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }
    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    client
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

async fn handle_store_error(
    e: WorterbuchError,
    client: &mpsc::Sender<ServerMessage>,
    transaction_id: u64,
) -> WorterbuchResult<()> {
    let error_code = ErrorCode::from(&e);
    let err_msg = match e {
        WorterbuchError::IllegalWildcard(pattern) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&pattern).expect("failed to serialize metadata"),
        },
        WorterbuchError::IllegalMultiWildcard(pattern) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&pattern).expect("failed to serialize metadata"),
        },
        WorterbuchError::MultiWildcardAtIllegalPosition(pattern) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&pattern).expect("failed to serialize metadata"),
        },
        WorterbuchError::NoSuchValue(key) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!("no value for key '{key}'"))
                .expect("failed to serialize error message"),
        },
        WorterbuchError::NotSubscribed => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!(
                "no subscription found for transaction id '{transaction_id}'"
            ))
            .expect("failed to serialize error message"),
        },
        WorterbuchError::IoError(e, meta) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string::<Meta>(&(&e.into(), meta).into())
                .expect("failed to serialize metadata"),
        },
        WorterbuchError::SerDeError(e, meta) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string::<Meta>(&(&e.into(), meta).into())
                .expect("failed to serialize metadata"),
        },
        WorterbuchError::ProtocolNegotiationFailed => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(
                "server does not implement any of the protocl versions supported by this client",
            )
            .expect("failed to serialize metadata"),
        },
        WorterbuchError::Other(e, meta) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string::<Meta>(&(&e, meta).into())
                .expect("failed to serialize metadata"),
        },
        WorterbuchError::ServerResponse(_) | WorterbuchError::InvalidServerResponse(_) => {
            panic!("store must not produce this error")
        }
        WorterbuchError::ReadOnlyKey(key) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!("tried to delete read only key '{key}'"))
                .expect("failed to serialize error message"),
        },
        WorterbuchError::AuthenticationFailed => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!("client failed to authenticate"))
                .expect("failed to serialize error message"),
        },
        WorterbuchError::AuthenticationRequired(op) => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!("operation {op} requires authentication"))
                .expect("failed to serialize error message"),
        },
        WorterbuchError::AlreadyAuthenticated => Err {
            error_code,
            transaction_id,
            metadata: serde_json::to_string(&format!(
                "handshake has already been completed, cannot do it again"
            ))
            .expect("failed to serialize error message"),
        },
    };
    client
        .send(ServerMessage::Err(err_msg))
        .await
        .context(|| format!("Error sending ERR message to client"))
}

#[derive(Serialize)]
struct Meta {
    cause: String,
    meta: MetaData,
}

impl From<(&Box<dyn std::error::Error + Send + Sync>, MetaData)> for Meta {
    fn from(e: (&Box<dyn std::error::Error + Send + Sync>, MetaData)) -> Self {
        Meta {
            cause: e.0.to_string(),
            meta: e.1,
        }
    }
}
