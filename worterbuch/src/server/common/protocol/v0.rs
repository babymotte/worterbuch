use std::time::Duration;

use crate::{
    auth::{get_claims, JwtClaims},
    server::common::{CloneableWbApi, SubscriptionInfo},
    subscribers::SubscriptionId,
    Config, PStateAggregator,
};
use serde_json::json;
use tokio::{spawn, sync::mpsc};
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    Ack, AuthorizationRequest, ClientMessage as CM, Delete, Err, ErrorCode, Get, Ls, LsState,
    PDelete, PGet, PLs, PState, PStateEvent, PSubscribe, Privilege, Publish, SPub, SPubInit,
    ServerMessage, Set, State, StateEvent, Subscribe, SubscribeLs, TransactionId, Unsubscribe,
    UnsubscribeLs,
};

#[derive(Debug, Clone, Default)]
pub struct V0 {}

impl V0 {
    pub async fn process_incoming_message(
        &self,
        client_id: Uuid,
        msg: CM,
        worterbuch: &CloneableWbApi,
        tx: &mpsc::Sender<ServerMessage>,
        auth_required: bool,
        authorized: &mut Option<JwtClaims>,
        config: &Config,
    ) -> WorterbuchResult<()> {
        match msg {
            CM::AuthorizationRequest(msg) => {
                if authorized.is_some() {
                    return Err(WorterbuchError::AlreadyAuthorized);
                }
                log::trace!("Authorizing client {client_id} …");
                *authorized = Some(authorize(msg, tx, config).await?);
                log::trace!("Authorizing client {client_id} done.");
            }
            CM::Get(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    &msg.key,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Getting value for client {} …", client_id);
                    get(msg, worterbuch, tx).await?;
                    log::trace!("Getting value for client {} done.", client_id);
                }
            }
            CM::PGet(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    &msg.request_pattern,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("PGetting values for client {} …", client_id);
                    pget(msg, worterbuch, tx).await?;
                    log::trace!("PGetting values for client {} done.", client_id);
                }
            }
            CM::Set(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Write,
                    &msg.key,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Setting value for client {} …", client_id);
                    set(msg, worterbuch, tx, client_id).await?;
                    log::trace!("Setting value for client {} done.", client_id);
                }
            }
            CM::SPubInit(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Write,
                    &msg.key,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!(
                        "Mapping key to transaction ID for for client {} …",
                        client_id
                    );
                    spub_init(msg, worterbuch, tx, client_id).await?;
                    log::trace!(
                        "Mapping key to transaction ID for client {} done.",
                        client_id
                    );
                }
            }
            CM::SPub(msg) => {
                log::trace!("Setting value for client {} …", client_id);
                spub(msg, worterbuch, tx, client_id).await?;
                log::trace!("Setting value for client {} done.", client_id);
            }
            CM::Publish(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Write,
                    &msg.key,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Publishing value for client {} …", client_id);
                    publish(msg, worterbuch, tx).await?;
                    log::trace!("Publishing value for client {} done.", client_id);
                }
            }
            CM::Subscribe(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    &msg.key,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Making subscription for client {} …", client_id);
                    subscribe(msg, client_id, worterbuch, tx).await?;
                    log::trace!("Making subscription for client {} done.", client_id);
                }
            }
            CM::PSubscribe(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    &msg.request_pattern,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Making psubscription for client {} …", client_id);
                    psubscribe(msg, client_id, worterbuch, tx).await?;
                    log::trace!("Making psubscription for client {} done.", client_id);
                }
            }
            CM::Unsubscribe(msg) => unsubscribe(msg, worterbuch, tx, client_id).await?,
            CM::Delete(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Delete,
                    &msg.key,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Deleting value for client {} …", client_id);
                    delete(msg, worterbuch, tx, client_id).await?;
                    log::trace!("Deleting value for client {} done.", client_id);
                }
            }
            CM::PDelete(msg) => {
                if check_auth(
                    auth_required,
                    Privilege::Delete,
                    &msg.request_pattern,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("DPeleting value for client {} …", client_id);
                    pdelete(msg, worterbuch, tx, client_id).await?;
                    log::trace!("DPeleting value for client {} done.", client_id);
                }
            }
            CM::Ls(msg) => {
                let pattern = &msg
                    .parent
                    .as_ref()
                    .map(|it| format!("{it}/?"))
                    .unwrap_or("?".to_owned());
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    pattern,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Listing subkeys for client {} …", client_id);
                    ls(msg, worterbuch, tx).await?;
                    log::trace!("Listing subkeys for client {} done.", client_id);
                }
            }
            CM::PLs(msg) => {
                let pattern = &msg
                    .parent_pattern
                    .as_ref()
                    .map(|it| format!("{it}/?"))
                    .unwrap_or("?".to_owned());
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    pattern,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Listing matching subkeys for client {} …", client_id);
                    pls(msg, worterbuch, tx).await?;
                    log::trace!("Listing matching subkeys for client {} done.", client_id);
                }
            }
            CM::SubscribeLs(msg) => {
                let pattern = &msg
                    .parent
                    .as_ref()
                    .map(|it| format!("{it}/?"))
                    .unwrap_or("?".to_owned());
                if check_auth(
                    auth_required,
                    Privilege::Read,
                    pattern,
                    authorized,
                    tx,
                    msg.transaction_id,
                )
                .await?
                {
                    log::trace!("Subscribing to subkeys for client {} …", client_id);
                    subscribe_ls(msg, client_id, worterbuch, tx).await?;
                    log::trace!("Subscribing to subkeys for client {} done.", client_id);
                }
            }
            CM::UnsubscribeLs(msg) => {
                log::trace!("Unsubscribing from subkeys for client {} …", client_id);
                unsubscribe_ls(msg, client_id, worterbuch, tx).await?;
                log::trace!("Unsubscribing from subkeys for client {} done.", client_id);
            }

            CM::ProtocolSwitchRequest(_) | CM::CGet(_) | CM::CSet(_) | CM::Transform(_) => {
                return Err(WorterbuchError::NotImplemented)
            }
        };
        Ok(())
    }
}

async fn authorize(
    msg: AuthorizationRequest,
    client: &mpsc::Sender<ServerMessage>,
    config: &Config,
) -> WorterbuchResult<JwtClaims> {
    match get_claims(Some(&msg.auth_token), config) {
        Ok(claims) => {
            client
                .send(ServerMessage::Authorized(Ack { transaction_id: 0 }))
                .await
                .context(|| "Error sending HANDSHAKE message".to_owned())?;
            Ok(claims)
        }
        Err(e) => {
            handle_store_error(WorterbuchError::Unauthorized(e.clone()), client, 0).await?;
            Err(WorterbuchError::Unauthorized(e))
        }
    }
}

pub async fn check_auth(
    auth_required: bool,
    privilege: Privilege,
    pattern: &str,
    auth: &Option<JwtClaims>,
    client: &mpsc::Sender<ServerMessage>,
    transaction_id: TransactionId,
) -> WorterbuchResult<bool> {
    if auth_required {
        match auth {
            Some(claims) => {
                if let Err(e) = claims.authorize(&privilege, pattern) {
                    log::trace!("Client is not authorized, sending error …");
                    handle_store_error(
                        WorterbuchError::Unauthorized(e.clone()),
                        client,
                        transaction_id,
                    )
                    .await?;
                    log::trace!("Client is not authorized, sending error done.");
                    return Ok(false);
                }
            }
            None => return Err(WorterbuchError::AuthorizationRequired(privilege)),
        }
    }
    Ok(true)
}

pub async fn get(
    msg: Get,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let value = match worterbuch.get(msg.key).await {
        Ok(it) => it,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = State {
        transaction_id: msg.transaction_id,
        event: StateEvent::Value(value),
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

pub async fn pget(
    msg: PGet,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let values = match worterbuch.pget(msg.request_pattern.clone()).await {
        Ok(values) => values.into_iter().collect(),
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

pub async fn set(
    msg: Set,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch.set(msg.key, msg.value, client_id).await {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    log::trace!("Value set, queuing Ack …");
    let res = client.send(ServerMessage::Ack(response)).await;
    log::trace!("Value set, queuing Ack done.");
    res.context(|| {
        format!(
            "Error sending ACK message for transaction ID {}",
            msg.transaction_id
        )
    })?;

    Ok(())
}

pub async fn spub_init(
    msg: SPubInit,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch
        .spub_init(msg.transaction_id, msg.key, client_id)
        .await
    {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    log::trace!("Value set, queuing Ack …");
    let res = client.send(ServerMessage::Ack(response)).await;
    log::trace!("Value set, queuing Ack done.");
    res.context(|| {
        format!(
            "Error sending ACK message for transaction ID {}",
            msg.transaction_id
        )
    })?;

    Ok(())
}

pub async fn spub(
    msg: SPub,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    if let Err(e) = worterbuch
        .spub(msg.transaction_id, msg.value, client_id)
        .await
    {
        handle_store_error(e, client, msg.transaction_id).await?;
        return Ok(());
    }

    let response = Ack {
        transaction_id: msg.transaction_id,
    };

    log::trace!("Value set, queuing Ack …");
    let res = client.send(ServerMessage::Ack(response)).await;
    log::trace!("Value set, queuing Ack done.");
    res.context(|| {
        format!(
            "Error sending ACK message for transaction ID {}",
            msg.transaction_id
        )
    })?;

    Ok(())
}

pub async fn publish(
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

pub async fn subscribe(
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
            let state = State {
                transaction_id,
                event,
            };
            if let Err(e) = client_sub.send(ServerMessage::State(state)).await {
                log::error!("Error sending STATE message to client: {e}");
                break;
            };
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

pub async fn psubscribe(
    msg: PSubscribe,
    client_id: Uuid,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<bool> {
    let live_only = msg.live_only.unwrap_or(false);

    let (rx, subscription) = match worterbuch
        .psubscribe(
            client_id,
            msg.transaction_id,
            msg.request_pattern.clone(),
            msg.unique,
            live_only,
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

    let channel_buffer_size = worterbuch.config().await?.channel_buffer_size;

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
                    log::warn!("Subscription was not cleaned up properly!");
                }
                Err(WorterbuchError::NotSubscribed) => { /* this is expected */ }
                Err(e) => {
                    log::warn!("Error while unsubscribing: {e}");
                }
            }
        });
    } else {
        spawn(async move {
            forward_loop(
                rx,
                transaction_id,
                request_pattern,
                client_sub,
                subscription,
            )
            .await;

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
    }

    Ok(true)
}

async fn forward_loop(
    mut rx: mpsc::Receiver<PStateEvent>,
    transaction_id: TransactionId,
    request_pattern: String,
    client_sub: mpsc::Sender<ServerMessage>,
    subscription: SubscriptionId,
) {
    log::debug!("Receiving events for subscription {subscription:?} …");
    while let Some(event) = rx.recv().await {
        let event = PState {
            transaction_id,
            request_pattern: request_pattern.clone(),
            event,
        };
        if let Err(e) = client_sub.send(ServerMessage::PState(event)).await {
            log::error!("Error sending STATE message to client: {e}");
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
        log::debug!("Immediately forwarding current state to new subscription {subscription:?} …");

        if let Some(event) = rx.recv().await {
            let event = PState {
                transaction_id: subscription.transaction_id,
                request_pattern: subscription.request_pattern.clone(),
                event,
            };

            if let Err(e) = client_sub.send(ServerMessage::PState(event)).await {
                log::error!("Error sending STATE message to client: {e}");
                return;
            }
        } else {
            return;
        }
    }

    log::debug!("Aggregating events for subscription {subscription:?} …");

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
            log::error!("Error sending STATE message to client: {e}");
            break;
        }
    }
}

pub async fn unsubscribe(
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

pub async fn delete(
    msg: Delete,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    let value = match worterbuch.delete(msg.key, client_id).await {
        Ok(it) => it,
        Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
            return Ok(());
        }
    };

    let response = State {
        transaction_id: msg.transaction_id,
        event: StateEvent::Deleted(value),
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

pub async fn pdelete(
    msg: PDelete,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
    client_id: Uuid,
) -> WorterbuchResult<()> {
    let deleted = match worterbuch
        .pdelete(msg.request_pattern.clone(), client_id)
        .await
    {
        Ok(it) => it,
        Result::Err(e) => {
            handle_store_error(e, client, msg.transaction_id).await?;
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

pub async fn ls(
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

pub async fn pls(
    msg: PLs,
    worterbuch: &CloneableWbApi,
    client: &mpsc::Sender<ServerMessage>,
) -> WorterbuchResult<()> {
    let children = match worterbuch.pls(msg.parent_pattern).await {
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

pub async fn subscribe_ls(
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
                transaction_id,
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

pub async fn unsubscribe_ls(
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

pub async fn handle_store_error(
    e: WorterbuchError,
    client: &mpsc::Sender<ServerMessage>,
    transaction_id: TransactionId,
) -> WorterbuchResult<()> {
    let error_code = ErrorCode::from(&e);
    let err_msg = format!("{e}");
    let err = Err {
        error_code,
        transaction_id,
        metadata: json!(err_msg).to_string(),
    };
    log::trace!("Error in store, queuing error message for client …");
    let res = client
        .send(ServerMessage::Err(err))
        .await
        .context(|| "Error sending ERR message to client".to_owned());
    log::trace!("Error in store, queuing error message for client done");
    res
}
