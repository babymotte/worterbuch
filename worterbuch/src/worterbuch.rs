/*
 *  Worterbuch core module
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

#[cfg(not(feature = "jemalloc"))]
use crate::mem_tools;
use crate::{
    INTERNAL_CLIENT_ID,
    config::Config,
    persistence::{PersistentStorageImpl, error::PersistenceResult},
    store::{PersistedStore, SerializeableLockNode, Store, StoreNode},
    subscribers::{EventSender, LsSubscriber, Subscriber, Subscribers},
};
use chrono::prelude::{DateTime, Utc};
use hashbrown::{HashMap, hash_map::Entry};
use hashlink::LinkedHashMap;
use serde_json::{Value, from_str, json};
use std::{
    io, mem,
    net::SocketAddr,
    ops::Deref,
    time::{Duration, SystemTime},
};
use tokio::{
    select, spawn,
    sync::{
        mpsc::{self, Receiver, channel},
        oneshot,
    },
    time::sleep,
};
use tracing::{Instrument, Level, debug, debug_span, error, info, instrument, trace, warn};
use worterbuch_common::{
    KeySegment, LsSubscription, PSubscription, Protocol, RegularKeySegment, Subscription,
    SubscriptionId, ValueEntry,
    error::{WorterbuchError, WorterbuchResult},
    parse_segments,
    protocol::{
        CasVersion, ClientId, GraveGoods, Interface, InternalAction, Key, KeyValuePair,
        KeyValuePairs, LastWill, Method, PState, PStateEvent, ProtocolMajorVersion, RequestPattern,
        SYSTEM_TOPIC_CLIENT_NAME, SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_CLIENTS_ADDRESS,
        SYSTEM_TOPIC_CLIENTS_PROTOCOL, SYSTEM_TOPIC_CLIENTS_PROTOCOL_VERSION,
        SYSTEM_TOPIC_CLIENTS_TIMESTAMP, SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL,
        SYSTEM_TOPIC_LOCKS, SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_ROOT_PREFIX,
        SYSTEM_TOPIC_SUBSCRIPTIONS, ServerMessage, StateEvent, Trace, TraceData, TransactionId,
    },
    topic,
};

pub type Subscriptions = HashMap<SubscriptionId, Vec<KeySegment>>;
pub type LsSubscriptions = HashMap<SubscriptionId, Vec<RegularKeySegment>>;

type Map<K, V> = LinkedHashMap<K, V>;

pub struct SubscriptionFlags {
    unique: bool,
    live_only: bool,
    send_traces: bool,
}

impl SubscriptionFlags {
    pub fn new(unique: bool, live_only: bool, send_traces: bool) -> Self {
        Self {
            unique,
            live_only,
            send_traces,
        }
    }
}

#[derive(Debug)]
struct ClientInfo {
    subscriptions: usize,
}

impl ClientInfo {
    fn new() -> Self {
        Self { subscriptions: 0 }
    }
}

struct PStateAggregatorState {
    aggregate_duration: Duration,
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    set_buffer: Map<Key, Value>,
    deleted_buffer: Map<Key, Value>,
    client_sub: mpsc::Sender<ServerMessage>,
    send_is_scheduled: bool,
}

impl PStateAggregatorState {
    async fn aggregate_loop(
        mut self,
        mut aggregate_rx: Receiver<PStateEvent>,
        client_id: ClientId,
    ) {
        let (send_trigger_tx, mut send_trigger_rx) = mpsc::channel::<()>(1);

        loop {
            select! {
                event = aggregate_rx.recv() => if let Some(event) = event {
                    if let Err(e) = self.aggregate(event, &send_trigger_tx, client_id).await {
                        error!("Error aggregating PState event for client {client_id}: {e}");
                        break;
                    }
                } else {
                    break;
                },
                tick = send_trigger_rx.recv() => if tick.is_some() {
                    if let Err(e) = self.send_current_state().await {
                        error!("Error sending PState event to client {client_id}: {e}");
                        break;
                    }
                } else {
                    break;
                },
            }
        }
    }

    async fn aggregate(
        &mut self,
        event: PStateEvent,
        send_trigger_tx: &mpsc::Sender<()>,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        if !self.send_is_scheduled {
            self.schedule_send(send_trigger_tx.clone(), self.aggregate_duration, client_id);
        }

        match event {
            PStateEvent::KeyValuePairs(kvps) => {
                if !self.deleted_buffer.is_empty() || self.key_already_buffered(&kvps) {
                    self.send_current_state().await?;
                }

                for kvp in kvps {
                    self.set_buffer.insert(kvp.key, kvp.value);
                }
            }
            PStateEvent::Deleted(kvps) => {
                if !self.set_buffer.is_empty() || self.key_already_buffered(&kvps) {
                    self.send_current_state().await?;
                }

                for kvp in kvps {
                    self.deleted_buffer.insert(kvp.key, kvp.value);
                }
            }
        }

        Ok(())
    }

    async fn send_current_state(&mut self) -> WorterbuchResult<()> {
        self.send_is_scheduled = false;

        if !self.set_buffer.is_empty() {
            self.send_set_event().await?;
        }

        if !self.deleted_buffer.is_empty() {
            self.send_deleted_event().await?;
        }

        Ok(())
    }

    async fn send_set_event(&mut self) -> WorterbuchResult<()> {
        let kvps: KeyValuePairs = self.set_buffer.drain().map(Into::into).collect();
        let event = PStateEvent::KeyValuePairs(kvps);
        self.send_aggregated_pstate(event).await?;
        Ok(())
    }

    async fn send_deleted_event(&mut self) -> WorterbuchResult<()> {
        let kvps: KeyValuePairs = self.deleted_buffer.drain().map(Into::into).collect();
        let event = PStateEvent::Deleted(kvps);
        self.send_aggregated_pstate(event).await?;
        Ok(())
    }

    async fn send_aggregated_pstate(&mut self, event: PStateEvent) -> Result<(), WorterbuchError> {
        let pstate = PState {
            transaction_id: self.transaction_id,
            request_pattern: self.request_pattern.clone(),
            event,
            trace: None,
        };
        self.client_sub.send(ServerMessage::PState(pstate)).await?;
        Ok(())
    }

    fn key_already_buffered(&self, kvps: &KeyValuePairs) -> bool {
        kvps.iter()
            .any(|kvp| self.set_buffer.contains_key(&kvp.key))
            || kvps
                .iter()
                .any(|kvp| self.deleted_buffer.contains_key(&kvp.key))
    }

    fn schedule_send(
        &mut self,
        send_trigger: mpsc::Sender<()>,
        aggregate_duration: Duration,
        client_id: ClientId,
    ) {
        self.send_is_scheduled = true;
        spawn(async move {
            sleep(aggregate_duration).await;
            if let Err(e) = send_trigger.send(()).await {
                debug!("Error triggering send of aggregated PState to client {client_id}: {e}");
            }
        });
    }
}

pub struct PStateAggregator {
    aggregate: mpsc::Sender<PStateEvent>,
}

impl PStateAggregator {
    pub fn new(
        client_sub: mpsc::Sender<ServerMessage>,
        request_pattern: RequestPattern,
        aggregate_duration: Duration,
        transaction_id: TransactionId,
        channel_buffer_size: usize,
        client_id: ClientId,
    ) -> Self {
        let aggregator_state = PStateAggregatorState {
            aggregate_duration,
            request_pattern,
            client_sub,
            set_buffer: Map::new(),
            deleted_buffer: Map::new(),
            send_is_scheduled: false,
            transaction_id,
        };

        let (aggregate_tx, aggregate_rx) = mpsc::channel(channel_buffer_size);

        spawn(aggregator_state.aggregate_loop(aggregate_rx, client_id));

        PStateAggregator {
            aggregate: aggregate_tx,
        }
    }

    pub async fn aggregate(&self, event: PStateEvent) -> WorterbuchResult<()> {
        self.aggregate.send(event).await?;
        Ok(())
    }
}

pub struct Worterbuch {
    config: Config,
    store: Store,
    subscriptions: Subscriptions,
    ls_subscriptions: LsSubscriptions,
    subscribers: Subscribers,
    clients: HashMap<ClientId, ClientInfo>,
    spub_keys: HashMap<ClientId, HashMap<TransactionId, (Key, Interface)>>,
    persistent_storage: PersistentStorageImpl,
}

impl Worterbuch {
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn with_config(config: Config) -> Worterbuch {
        Worterbuch {
            config,
            clients: Default::default(),
            ls_subscriptions: Default::default(),
            store: Default::default(),
            subscribers: Default::default(),
            subscriptions: Default::default(),
            spub_keys: Default::default(),
            persistent_storage: Default::default(),
        }
    }

    #[instrument(skip(store, config))]
    pub fn from_persistence(store: PersistedStore, config: Config) -> Worterbuch {
        let store: Store = store.into();
        debug!("Loaded persisted store with {} entries.", store.len());
        Worterbuch {
            config,
            store,
            clients: Default::default(),
            ls_subscriptions: Default::default(),
            subscribers: Default::default(),
            subscriptions: Default::default(),
            spub_keys: Default::default(),
            persistent_storage: Default::default(),
        }
    }

    #[instrument(skip(store, config))]
    #[cfg(any(feature = "redb", feature = "sqlite", feature = "turso"))]
    pub fn with_store(store: Store, config: Config) -> Worterbuch {
        debug!("Loaded persisted store with {} entries.", store.len());
        Worterbuch {
            config,
            store,
            clients: Default::default(),
            ls_subscriptions: Default::default(),
            subscribers: Default::default(),
            subscriptions: Default::default(),
            spub_keys: Default::default(),
            persistent_storage: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn get(&self, key: &Key) -> WorterbuchResult<Value> {
        let path: Vec<RegularKeySegment> = parse_segments(key)?;

        match self.store.get(&path) {
            Some(value) => Ok(value.to_owned()),
            None => Err(WorterbuchError::NoSuchValue(key.to_owned())),
        }
    }

    pub fn cget(&self, key: &Key) -> WorterbuchResult<(Value, CasVersion)> {
        let path: Vec<RegularKeySegment> = parse_segments(key)?;

        match self.store.cget(&path) {
            Some((value, version)) => Ok((value.to_owned(), version)),
            None => Err(WorterbuchError::NoSuchValue(key.to_owned())),
        }
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub async fn set(
        &mut self,
        key: Key,
        value: Value,
        force: bool,
        trace_data: TraceData,
    ) -> WorterbuchResult<()> {
        let cause = Trace::client_request(Method::Set, &trace_data);
        self.internal_set(key, value, trace_data.client_id, cause, force)
            .await
    }

    #[instrument(level = Level::TRACE, skip(self))]
    pub(crate) async fn internal_set(
        &mut self,
        key: Key,
        value: Value,
        client_id: ClientId,
        cause: Trace,
        force: bool,
    ) -> WorterbuchResult<()> {
        check_for_read_only_key(&key, client_id)?;

        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        let (changed, ls_subscribers) = self.store.insert_plain(&path, value.clone(), force)?;

        self.persistent_storage
            .update_value(&key, &ValueEntry::Plain(value.clone()), Some(client_id))
            .await
            .map_err(|e| {
                WorterbuchError::IoError(
                    io::Error::other(e),
                    "Failed to insert value into persistent storage".to_owned(),
                )
            })?;

        if let Some(ls_subscribers) = ls_subscribers {
            trace!("Notifying ls subscribers …");
            self.notify_ls_subscribers(ls_subscribers, cause.clone())
                .await;
            trace!("Notifying ls subscribers done.");
        }
        trace!("Notifying subscribers …");
        self.notify_subscribers(&path, &key, &value, changed, false, cause)
            .await;
        trace!("Notifying subscribers done.");

        Ok(())
    }

    pub async fn cset(
        &mut self,
        key: Key,
        value: Value,
        version: CasVersion,
        force: bool,
        trace_data: TraceData,
    ) -> WorterbuchResult<()> {
        let trace = Trace::client_request(Method::CSet, &trace_data);
        self.internal_cset(key, value, version, trace_data.client_id, trace, force)
            .await
    }

    pub(crate) async fn internal_cset(
        &mut self,
        key: Key,
        value: Value,
        version: CasVersion,
        client_id: ClientId,
        cause: Trace,
        force: bool,
    ) -> WorterbuchResult<()> {
        check_for_read_only_key(&key, client_id)?;

        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        let (changed, ls_subscribers) =
            self.store
                .insert_cas(&path, value.clone(), version, force)?;

        self.persistent_storage
            .update_value(
                &key,
                &ValueEntry::Cas(value.clone(), version),
                Some(client_id),
            )
            .await
            .map_err(|e| {
                WorterbuchError::IoError(
                    io::Error::other(e),
                    "Failed to insert value into persistent storage".to_owned(),
                )
            })?;

        if let Some(ls_subscribers) = ls_subscribers {
            trace!("Notifying ls subscribers …");
            self.notify_ls_subscribers(ls_subscribers, cause.clone())
                .await;
            trace!("Notifying ls subscribers done.");
        }
        trace!("Notifying subscribers …");
        self.notify_subscribers(&path, &key, &value, changed, false, cause)
            .await;
        trace!("Notifying subscribers done.");

        Ok(())
    }

    pub async fn spub_init(&mut self, key: Key, trace_data: TraceData) -> WorterbuchResult<()> {
        check_for_read_only_key(&key, trace_data.client_id)?;
        self.store_key(
            trace_data.client_id,
            trace_data.transaction_id,
            key,
            trace_data.interface.clone(),
        );

        Ok(())
    }

    pub async fn spub(
        &mut self,
        transaction_id: TransactionId,
        value: Value,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        if let Some((key, interface)) = self.lookup_key(client_id, transaction_id) {
            self.publish(
                key,
                value,
                TraceData {
                    client_id,
                    transaction_id,
                    interface,
                },
            )
            .await
        } else {
            Err(WorterbuchError::NoPubStream(transaction_id))
        }
    }

    pub async fn publish(
        &mut self,
        key: Key,
        value: Value,
        trace_data: TraceData,
    ) -> WorterbuchResult<()> {
        let path: Vec<RegularKeySegment> = parse_segments(&key)?;
        let trace = Trace::client_request(Method::Publish, &trace_data);

        self.notify_subscribers(&path, &key, &value, true, false, trace)
            .await;

        Ok(())
    }

    pub fn pget(&self, pattern: &str) -> WorterbuchResult<KeyValuePairs> {
        let path: Vec<KeySegment> = KeySegment::parse(pattern);
        Ok(self.store.get_matches(&path)?)
    }

    pub async fn subscribe(
        &mut self,
        key: Key,
        flags: SubscriptionFlags,
        trace_data: TraceData,
    ) -> WorterbuchResult<Subscription> {
        let trace = Trace::client_request(Method::Subscribe, &trace_data);
        self.internal_subscribe(
            trace_data.client_id,
            trace_data.transaction_id,
            trace,
            key,
            flags,
        )
        .await
    }

    pub(crate) async fn internal_subscribe(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        cause: Trace,
        key: Key,
        flags: SubscriptionFlags,
    ) -> WorterbuchResult<Subscription> {
        let path: Vec<KeySegment> = KeySegment::parse(&key);
        let (tx, rx) = channel(self.config.channel_buffer_size);
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone(),
            EventSender::State(tx.clone()),
            flags.unique,
            flags.send_traces,
        );
        self.subscribers.add_subscriber(&path, subscriber);

        if !flags.live_only {
            let matches = match self.get(&key) {
                Ok(value) => Some(value),
                Err(WorterbuchError::NoSuchValue(_)) => None,
                Err(e) => return Err(e),
            };
            if let Some(value) = matches {
                tx.send((StateEvent::Value(value), Some(cause.clone())))
                    .await
                    .expect("rx is neither closed nor dropped");
            }
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);

        let mut client_subs = 0;
        if let Some(client) = self.clients.get_mut(&client_id) {
            client_subs = client.subscriptions.saturating_add(1);
            client.subscriptions = client_subs;
        }

        debug!("Total subscriptions: {}", self.subscriptions.len());

        if self.config.extended_monitoring
            && key != SYSTEM_TOPIC_ROOT
            && !key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX)
            && client_id != INTERNAL_CLIENT_ID
        {
            if let Err(e) = self
                .internal_set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                    json!(self.subscriptions.len()),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::SubscriptionsChanged {
                        cause: Box::new(cause.clone()),
                    }),
                    true,
                )
                .await
            {
                debug!("Error in subscription monitoring: {e}");
            }

            let subs_key = topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_SUBSCRIPTIONS
            );
            if let Err(e) = self
                .internal_set(
                    topic!(subs_key, key),
                    json!(transaction_id),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::SubscriptionsChanged {
                        cause: Box::new(cause.clone()),
                    }),
                    true,
                )
                .await
            {
                debug!("Error in subscription monitoring: {e}");
            }
            self.update_subscription_count(subs_key, client_subs, cause)
                .await;
        }

        Ok((rx, subscription))
    }

    pub async fn psubscribe(
        &mut self,
        pattern: RequestPattern,
        flags: SubscriptionFlags,
        trace_data: TraceData,
    ) -> WorterbuchResult<PSubscription> {
        let trace = Trace::client_request(Method::PSubscribe, &trace_data);
        self.internal_psubscribe(
            trace_data.client_id,
            trace_data.transaction_id,
            trace,
            pattern,
            flags,
        )
        .await
    }

    pub(crate) async fn internal_psubscribe(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        cause: Trace,
        pattern: RequestPattern,
        flags: SubscriptionFlags,
    ) -> WorterbuchResult<PSubscription> {
        let path: Vec<KeySegment> = KeySegment::parse(&pattern);
        let (tx, rx) = channel(self.config.channel_buffer_size);
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone().into_iter().map(|s| s.to_owned()).collect(),
            EventSender::PState(tx.clone()),
            flags.unique,
            flags.send_traces,
        );
        self.subscribers.add_subscriber(&path, subscriber);
        if !flags.live_only {
            let matches = self.pget(&pattern)?;
            tx.send((PStateEvent::KeyValuePairs(matches), Some(cause.clone())))
                .await
                .expect("rx is neither closed nor dropped");
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);
        let mut client_subs = 0;
        if let Some(client) = self.clients.get_mut(&client_id) {
            client_subs = client.subscriptions.saturating_add(1);
            client.subscriptions = client_subs;
        }

        debug!("Total subscriptions: {}", self.subscriptions.len());

        if self.config.extended_monitoring
            && pattern != "#"
            && pattern != SYSTEM_TOPIC_ROOT
            && !pattern.starts_with(SYSTEM_TOPIC_ROOT_PREFIX)
            && client_id != INTERNAL_CLIENT_ID
        {
            if let Err(e) = self
                .internal_set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                    json!(self.subscriptions.len()),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::SubscriptionsChanged {
                        cause: Box::new(cause.clone()),
                    }),
                    true,
                )
                .await
            {
                debug!("Error in subscription monitoring: {e}");
            }
            let subs_key = topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_SUBSCRIPTIONS
            );
            if let Err(e) = self
                .internal_set(
                    topic!(subs_key, escape_wildcards(&pattern)),
                    json!(transaction_id),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::SubscriptionsChanged {
                        cause: Box::new(cause.clone()),
                    }),
                    true,
                )
                .await
            {
                debug!("Error in subscription monitoring: {e}");
            }
            self.update_subscription_count(subs_key, client_subs, cause)
                .await;
        }

        Ok((rx, subscription))
    }

    async fn update_subscription_count(
        &mut self,
        subs_key: String,
        client_subs: usize,
        cause: Trace,
    ) {
        if client_subs > 0 {
            if let Err(e) = self
                .internal_set(
                    topic!(subs_key),
                    json!(client_subs),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::SubscriptionsChanged {
                        cause: Box::new(cause.clone()),
                    }),
                    true,
                )
                .await
            {
                debug!("Error in subscription monitoring: {e}");
            }
        } else if let Err(e) = self
            .internal_delete(
                topic!(subs_key),
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::SubscriptionsChanged {
                    cause: Box::new(cause),
                }),
            )
            .await
        {
            debug!("Error in subscription monitoring: {e}");
        }
    }

    pub async fn subscribe_ls(
        &mut self,
        parent: Option<Key>,
        send_traces: bool,
        trace_data: TraceData,
    ) -> WorterbuchResult<LsSubscription> {
        let trace = Trace::client_request(Method::LsSubscribe, &trace_data);
        let children = self.ls(&parent).unwrap_or_else(|_| Vec::new());
        let path: Vec<RegularKeySegment> = parent
            .map(|p| p.split('/').map(ToOwned::to_owned).collect())
            .unwrap_or_default();
        let (tx, rx) = channel(self.config.channel_buffer_size);
        let subscription = SubscriptionId::new(trace_data.client_id, trace_data.transaction_id);
        let subscriber =
            LsSubscriber::new(subscription.clone(), path.clone(), tx.clone(), send_traces);
        self.store.add_ls_subscriber(&path, subscriber);
        tx.send((children, Some(trace.clone())))
            .await
            .expect("rx is neither closed nor dropped");
        let subscription_id = SubscriptionId::new(trace_data.client_id, trace_data.transaction_id);
        self.ls_subscriptions.insert(subscription_id, path);
        debug!("Total ls subscriptions: {}", self.ls_subscriptions.len());
        Ok((rx, subscription))
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    pub fn export(&mut self) -> (StoreNode, GraveGoods, LastWill) {
        let store = self.store.export();
        let grave_goods = self.grave_goods();
        let last_will = self.last_wills();

        (store, grave_goods, last_will)
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    pub(crate) fn export_with_locks(
        &mut self,
    ) -> (StoreNode, SerializeableLockNode, GraveGoods, LastWill) {
        let store = self.store.export();
        let locks = self.store.export_locks();
        let grave_goods = self.grave_goods();
        let last_will = self.last_wills();

        (store, locks, grave_goods, last_will)
    }

    #[instrument(level=Level::DEBUG, skip(self, tx))]
    pub fn export_for_persistence(&mut self, tx: oneshot::Sender<(Value, GraveGoods, LastWill)>) {
        let store = self.store.export_for_persistence();
        let grave_goods = self.grave_goods();
        let last_will = self.last_wills();
        let span = debug_span!("serialize_and_send");
        spawn(
            async { Self::serialize_and_send(store, grave_goods, last_will, tx) }.instrument(span),
        );
    }

    fn serialize_and_send(
        store: PersistedStore,
        grave_goods: GraveGoods,
        last_will: LastWill,
        tx: oneshot::Sender<(Value, GraveGoods, LastWill)>,
    ) {
        let value = json!(store);
        tx.send((value, grave_goods, last_will)).ok();
    }

    pub async fn import(
        &mut self,
        json: &str,
        client_id: ClientId,
        transaction_id: TransactionId,
        interface: Interface,
    ) -> WorterbuchResult<Vec<(String, (ValueEntry, bool))>> {
        debug!("Parsing store data …");
        let store: PersistedStore = from_str(json).map_err(|e| {
            WorterbuchError::SerDeError(e, "Error parsing JSON during import".to_owned())
        })?;
        debug!("Done. Merging nodes …");
        let imported_values = self.store.merge(store.data);

        let trace = Trace::ClientRequest {
            client_id,
            transaction_id,
            method: Method::Import,
            interface,
        };

        for (key, (val, changed)) in &imported_values {
            if *changed {
                self.persistent_storage
                    .update_value(key, val, None)
                    .await
                    .map_err(|e| {
                        WorterbuchError::IoError(
                            io::Error::other(e),
                            "Failed to insert value into persistent storage".to_owned(),
                        )
                    })?;
            }

            let path: Vec<RegularKeySegment> = parse_segments(key)?;

            self.notify_subscribers(&path, key, val.as_ref(), *changed, false, trace.clone())
                .await;
        }

        Ok(imported_values)
    }

    pub async fn unsubscribe(&mut self, trace_data: TraceData) -> WorterbuchResult<()> {
        let trace = Trace::client_request(Method::Unsubscribe, &trace_data);
        let subscription = SubscriptionId::new(trace_data.client_id, trace_data.transaction_id);
        self.do_unsubscribe(&subscription, trace_data.client_id, trace)
            .await
    }

    async fn do_unsubscribe(
        &mut self,
        subscription: &SubscriptionId,
        client_id: ClientId,
        cause: Trace,
    ) -> WorterbuchResult<()> {
        if let Some(path) = self.subscriptions.remove(subscription) {
            let mut client_subs = 0;
            if let Some(client) = self.clients.get_mut(&client_id) {
                client_subs = client.subscriptions.saturating_sub(1);
                client.subscriptions = client_subs;
            }
            if self.config.extended_monitoring
                && path[0] != KeySegment::MultiWildcard
                && path[0].deref() != SYSTEM_TOPIC_ROOT
                && client_id != INTERNAL_CLIENT_ID
            {
                let subs_key = topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_SUBSCRIPTIONS
                );
                if let Err(e) = self
                    .internal_delete(
                        topic!(
                            subs_key,
                            escape_wildcards(
                                &path
                                    .iter()
                                    .map(ToString::to_string)
                                    .collect::<Vec<String>>()
                                    .join("/")
                            )
                        ),
                        INTERNAL_CLIENT_ID,
                        Trace::InternalAction(InternalAction::SubscriptionsChanged {
                            cause: Box::new(cause.clone()),
                        }),
                    )
                    .await
                {
                    match e {
                        WorterbuchError::NoSuchValue(_) => (/* will happen on disconnect */),
                        _ => debug!("Error in subscription monitoring: {e}"),
                    }
                }
                self.update_subscription_count(subs_key, client_subs, cause.clone())
                    .await;
            }
            debug!("Remaining subscriptions: {}", self.subscriptions.len());

            if self.config.extended_monitoring
                && client_id != INTERNAL_CLIENT_ID
                && let Err(e) = self
                    .internal_set(
                        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                        json!(self.subscriptions.len()),
                        INTERNAL_CLIENT_ID,
                        Trace::InternalAction(InternalAction::SubscriptionsChanged {
                            cause: Box::new(cause),
                        }),
                        true,
                    )
                    .await
            {
                debug!("Error in subscription monitoring: {e}");
            }
            if self.subscribers.unsubscribe(&path, subscription) {
                Ok(())
            } else {
                Err(WorterbuchError::NotSubscribed)
            }
        } else {
            Err(WorterbuchError::NotSubscribed)
        }
    }

    pub fn unsubscribe_ls(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let subscription = SubscriptionId::new(client_id, transaction_id);
        self.do_unsubscribe_ls(&subscription)
    }

    fn do_unsubscribe_ls(&mut self, subscription: &SubscriptionId) -> WorterbuchResult<()> {
        if let Some(path) = self.ls_subscriptions.remove(subscription) {
            debug!(
                "Remaining ls subscriptions: {}",
                self.ls_subscriptions.len()
            );
            if self.store.unsubscribe_ls(&path, subscription) {
                Ok(())
            } else {
                Err(WorterbuchError::NotSubscribed)
            }
        } else {
            Err(WorterbuchError::NotSubscribed)
        }
    }

    async fn notify_subscribers(
        &mut self,
        path: &[RegularKeySegment],
        key: &Key,
        value: &Value,
        value_changed: bool,
        deleted: bool,
        trace: Trace,
    ) {
        let subscribers = self.subscribers.get_subscribers(path);

        let filtered_subscribers: Vec<Subscriber> = subscribers
            .into_iter()
            .filter(|s| value_changed || !s.is_unique())
            .collect();

        let len = filtered_subscribers.len();
        trace!("Calling {} subscribers: {} = {:?} …", len, key, value);
        for subscriber in filtered_subscribers {
            if subscriber.is_pstate_subscriber() {
                let kvps = vec![KeyValuePair::of(key, value)];
                if let Err(e) = if deleted {
                    subscriber
                        .send_pstate(PStateEvent::Deleted(kvps), trace.clone())
                        .await
                } else {
                    subscriber
                        .send_pstate(PStateEvent::KeyValuePairs(kvps), trace.clone())
                        .await
                } {
                    debug!("Error calling subscriber: {e}");
                    self.subscribers.remove_subscriber(subscriber);
                }
            } else {
                let value = value.to_owned();
                if let Err(e) = if deleted {
                    subscriber
                        .send_state(StateEvent::Deleted(value), trace.clone())
                        .await
                } else {
                    subscriber
                        .send_state(StateEvent::Value(value), trace.clone())
                        .await
                } {
                    debug!("Error calling subscriber: {e}");
                    self.subscribers.remove_subscriber(subscriber);
                }
            }
        }
        trace!("Calling {} subscribers: {} = {:?} done.", len, key, value);
    }

    async fn notify_ls_subscribers(
        &mut self,
        ls_subscribers: Vec<(Vec<LsSubscriber>, Vec<String>)>,
        trace: Trace,
    ) {
        let len = ls_subscribers.len();
        trace!("Calling {} ls subscribers …", len);
        for (subscribers, new_children) in ls_subscribers {
            for subscriber in subscribers {
                if let Err(e) = subscriber.send(new_children.clone(), trace.clone()).await {
                    debug!("Error calling subscriber: {e}");
                    self.store.remove_ls_subscriber(subscriber);
                }
            }
        }
        trace!("Calling {} ls subscribers done.", len);
    }

    pub async fn delete(
        &mut self,
        key: Key,
        client_id: ClientId,
        transaction_id: TransactionId,
        interface: Interface,
    ) -> WorterbuchResult<Value> {
        let cause = Trace::ClientRequest {
            client_id,
            transaction_id,
            method: Method::Delete,
            interface,
        };
        self.internal_delete(key, client_id, cause).await
    }

    pub(crate) async fn internal_delete(
        &mut self,
        key: Key,
        client_id: ClientId,
        cause: Trace,
    ) -> WorterbuchResult<Value> {
        check_for_read_only_key(&key, client_id)?;

        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        match self.store.delete(&path)? {
            Some((value, ls_subscribers)) => {
                self.persistent_storage
                    .delete_value(&key)
                    .await
                    .map_err(|e| {
                        WorterbuchError::IoError(
                            io::Error::other(e),
                            "Failed to remove value from persistent storage".to_owned(),
                        )
                    })?;

                if let Some(ls_subscribers) = ls_subscribers {
                    self.notify_ls_subscribers(ls_subscribers, cause.clone())
                        .await;
                }
                self.notify_subscribers(&path, &key, &value, true, true, cause)
                    .await;
                Ok(value)
            }
            None => Err(WorterbuchError::NoSuchValue(key)),
        }
    }

    pub async fn pdelete(
        &mut self,
        pattern: RequestPattern,
        client_id: ClientId,
        transaction_id: TransactionId,
        interface: Interface,
    ) -> WorterbuchResult<KeyValuePairs> {
        let cause = Trace::ClientRequest {
            client_id,
            transaction_id,
            method: Method::PDelete,
            interface,
        };
        self.internal_pdelete(pattern, false, client_id, cause)
            .await
    }

    pub(crate) async fn internal_pdelete(
        &mut self,
        pattern: RequestPattern,
        skip_read_only_check: bool,
        client_id: ClientId,
        cause: Trace,
    ) -> Result<Vec<KeyValuePair>, WorterbuchError> {
        if !skip_read_only_check {
            check_for_read_only_key(&pattern, client_id)?;
        }

        let path: Vec<KeySegment> = KeySegment::parse(&pattern);

        let (deleted, ls_subscribers) = self.store.delete_matches(&path)?;

        for kvp in &deleted {
            self.persistent_storage
                .delete_value(&kvp.key)
                .await
                .map_err(|e| {
                    WorterbuchError::IoError(
                        io::Error::other(e),
                        "Failed to remove value from persistent storage".to_owned(),
                    )
                })?;

            let path = parse_segments(&kvp.key)?;
            self.notify_subscribers(&path, &kvp.key, &kvp.value, true, true, cause.clone())
                .await;
        }
        if let Some(ls_subscribers) = ls_subscribers {
            self.notify_ls_subscribers(ls_subscribers, cause).await;
        }

        #[cfg(not(feature = "jemalloc"))]
        mem_tools::schedule_trim();

        Ok(deleted)
    }

    pub async fn lock(
        &mut self,
        key: Key,
        client_id: ClientId,
        transaction_id: TransactionId,
        interface: Interface,
    ) -> WorterbuchResult<()> {
        let path: Box<[RegularKeySegment]> = parse_segments(&key)?.into();

        let trace = Trace::ClientRequest {
            client_id,
            transaction_id,
            method: Method::Lock,
            interface,
        };

        self.store.lock(client_id, path)?;

        self.locked(Some(client_id), &key, trace).await;

        Ok(())
    }

    pub async fn acquire_lock(
        &mut self,
        key: Key,
        client_id: ClientId,
        transaction_id: TransactionId,
        interface: Interface,
    ) -> WorterbuchResult<oneshot::Receiver<()>> {
        let path: Box<[RegularKeySegment]> = parse_segments(&key)?.into();

        let trace = Trace::ClientRequest {
            client_id,
            transaction_id,
            method: Method::AcquireLock,
            interface,
        };

        let (rx, client_id) = self.store.acquire_lock(client_id, path).await;

        self.locked(client_id, &key, trace).await;

        Ok(rx)
    }

    async fn locked(&mut self, client_id: Option<ClientId>, key: &str, cause: Trace) {
        if self.config.extended_monitoring {
            if let Some(client_id) = client_id {
                if let Err(e) = self
                    .internal_set(
                        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_LOCKS, escape_wildcards(key)),
                        json!(client_id),
                        INTERNAL_CLIENT_ID,
                        Trace::InternalAction(InternalAction::LocksChanged {
                            cause: Box::new(cause.clone()),
                        }),
                        true,
                    )
                    .await
                {
                    error!("Error updating client locks: {e}");
                };
            } else if let Err(e) = self
                .internal_delete(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_LOCKS, escape_wildcards(key)),
                    INTERNAL_CLIENT_ID,
                    Trace::InternalAction(InternalAction::LocksChanged {
                        cause: Box::new(cause.clone()),
                    }),
                )
                .await
            {
                error!("Error updating client locks: {e}");
            }
        }
    }

    pub async fn release_lock(
        &mut self,
        key: Key,
        client_id: ClientId,
        transaction_id: TransactionId,
        interface: Interface,
    ) -> WorterbuchResult<()> {
        let path: Box<[RegularKeySegment]> = parse_segments(&key)?.into();

        let trace = Trace::ClientRequest {
            client_id,
            transaction_id,
            method: Method::ReleaseLock,
            interface,
        };

        let client_id = self.store.unlock(client_id, &path).await?;

        self.locked(client_id, &key, trace).await;

        Ok(())
    }

    pub fn ls(&self, parent: &Option<Key>) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let path = parent
            .as_deref()
            .map_or_else(Vec::new, |p| p.split('/').collect());
        self.ls_path(&path)
    }

    fn ls_path(&self, path: &[&str]) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let children = if path.is_empty() {
            Some(self.store.ls_root())
        } else {
            self.store.ls(path)
        };

        children.map_or_else(
            || Err(WorterbuchError::NoSuchValue(path.join("/"))),
            Result::Ok,
        )
    }

    pub fn pls(
        &self,
        parent_pattern: &Option<RequestPattern>,
    ) -> WorterbuchResult<Vec<RegularKeySegment>> {
        if let Some(parent_pattern) = parent_pattern {
            let path: Vec<KeySegment> = KeySegment::parse(parent_pattern);
            self.pls_path(&path)
        } else {
            self.ls(&None)
        }
    }

    fn pls_path(&self, path: &[KeySegment]) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let children = if path.is_empty() {
            Ok(self.store.ls_root())
        } else {
            self.store.pls(path)
        };

        children.map_or_else(
            |_| {
                Err(WorterbuchError::IllegalMultiWildcard(
                    path.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<String>>()
                        .join("/"),
                ))
            },
            Result::Ok,
        )
    }

    pub async fn connected(
        &mut self,
        client_id: ClientId,
        remote_addr: Option<SocketAddr>,
        protocol: Protocol,
    ) -> WorterbuchResult<()> {
        debug_assert!(client_id != INTERNAL_CLIENT_ID);

        if self.clients.contains_key(&client_id) {
            return Err(WorterbuchError::ClientIdCollision(client_id));
        }

        let now = SystemTime::now().into();

        self.clients.insert(client_id, ClientInfo::new());
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        let trace = Trace::InternalAction(InternalAction::ClientConnected {
            client_id,
            protocol: protocol.clone(),
        });
        if let Err(e) = self
            .internal_set(
                client_count_key,
                json!(self.clients.len()),
                INTERNAL_CLIENT_ID,
                trace.clone(),
                true,
            )
            .await
        {
            error!("Error updating client count: {e}");
        }
        if let Err(e) = self
            .set_client_protocol(&client_id, &protocol, trace.clone())
            .await
        {
            error!("Error updating client protocol: {e}");
        };

        if let Err(e) = self
            .set_client_address(&client_id, remote_addr, trace.clone())
            .await
        {
            error!("Error updating client address: {e}");
        }

        if self.config.extended_monitoring
            && client_id != INTERNAL_CLIENT_ID
            && let Err(e) = self.set_client_timestamp(&client_id, now, trace).await
        {
            error!("Error updating client timestamp: {e}");
        }

        Ok(())
    }

    pub async fn protocol_switched(
        &mut self,
        client_id: ClientId,
        interface: Interface,
        protocol: ProtocolMajorVersion,
    ) {
        let trace = Trace::ProtocolSwitch {
            client_id,
            protocol_version: protocol,
            interface,
        };
        self.internal_protocol_switched(client_id, protocol, trace)
            .await
    }

    pub(crate) async fn internal_protocol_switched(
        &mut self,
        client_id: ClientId,
        protocol: ProtocolMajorVersion,
        cause: Trace,
    ) {
        if self.clients.contains_key(&client_id)
            && self.config.extended_monitoring
            && client_id != INTERNAL_CLIENT_ID
            && let Err(e) = self
                .set_client_protocol_version(&client_id, protocol, cause)
                .await
        {
            error!("Error updating client protocol version: {e}");
        }
    }

    async fn set_client_protocol(
        &mut self,
        client_id: &ClientId,
        protocol: &Protocol,
        cause: Trace,
    ) -> WorterbuchResult<()> {
        self.internal_set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_PROTOCOL
            ),
            json!(protocol),
            INTERNAL_CLIENT_ID,
            cause,
            true,
        )
        .await
    }

    async fn set_client_protocol_version(
        &mut self,
        client_id: &ClientId,
        protocol_version: ProtocolMajorVersion,
        cause: Trace,
    ) -> WorterbuchResult<()> {
        self.internal_set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_PROTOCOL_VERSION
            ),
            json!(protocol_version),
            INTERNAL_CLIENT_ID,
            cause,
            true,
        )
        .await
    }

    async fn set_client_address(
        &mut self,
        client_id: &ClientId,
        remote_addr: Option<SocketAddr>,
        cause: Trace,
    ) -> WorterbuchResult<()> {
        let remote_addr = serde_json::to_value(remote_addr).map_err(|e| {
            WorterbuchError::SerDeError(e, "could not convert remote address to value".to_owned())
        })?;
        self.internal_set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_ADDRESS
            ),
            remote_addr,
            INTERNAL_CLIENT_ID,
            cause,
            true,
        )
        .await
    }

    async fn set_client_timestamp(
        &mut self,
        client_id: &ClientId,
        timestamp: DateTime<Utc>,
        cause: Trace,
    ) -> WorterbuchResult<()> {
        let timestamp = json!(timestamp.format("%+").to_string());
        self.internal_set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_TIMESTAMP
            ),
            timestamp,
            INTERNAL_CLIENT_ID,
            cause,
            true,
        )
        .await
    }

    fn grave_goods_for_client(&self, client_id: &ClientId) -> Option<GraveGoods> {
        let key = topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_CLIENTS,
            client_id,
            SYSTEM_TOPIC_GRAVE_GOODS
        );
        let value = self.get(&key).ok();
        value.and_then(|it| serde_json::from_value(it).ok())
    }

    fn last_will_for_client(&self, client_id: &ClientId) -> Option<LastWill> {
        let key = topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_CLIENTS,
            client_id,
            SYSTEM_TOPIC_LAST_WILL
        );
        let value = self.get(&key).ok();
        value.and_then(|it| serde_json::from_value(it).ok())
    }

    pub async fn disconnected(
        &mut self,
        client_id: ClientId,
        protocol: Protocol,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        debug_assert!(client_id != INTERNAL_CLIENT_ID);

        if let Some(spubs) = self.spub_keys.remove(&client_id) {
            info!(
                "Dropping {} pub stream(s) of client {}.",
                spubs.len(),
                client_id
            );
        }

        let trace = Trace::InternalAction(InternalAction::ClientDisconnected {
            client_id,
            protocol,
        });

        if let Some(keys) = self.store.unlock_all(client_id).await
            && self.config.extended_monitoring
            && !keys.is_empty()
        {
            info!("Dropping locks of client {}.", client_id);
            for (key, client_id) in keys {
                self.locked(client_id, &key, trace.clone()).await;
            }
        }

        let grave_goods = self.grave_goods_for_client(&client_id);
        let last_wills = self.last_will_for_client(&client_id);

        self.clients.remove(&client_id);
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        if let Err(e) = self
            .internal_set(
                client_count_key,
                json!(self.clients.len()),
                INTERNAL_CLIENT_ID,
                trace.clone(),
                true,
            )
            .await
        {
            error!("Error updating client count: {e}");
        }

        let subscription_keys: Vec<SubscriptionId> = self
            .subscriptions
            .keys()
            .filter(|k| k.client_id == client_id)
            .map(ToOwned::to_owned)
            .collect();
        if !subscription_keys.is_empty() {
            info!(
                "Removing {} subscription(s) of client {client_id} ({}).",
                subscription_keys.len(),
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );
        }
        for subscription in subscription_keys {
            if let Err(e) = self
                .do_unsubscribe(&subscription, client_id, trace.clone())
                .await
            {
                error!("Inconsistent subscription state: {e}");
            }
        }

        let pattern = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS, client_id, "#");
        debug!("Deleting {pattern}");
        if let Err(e) = self
            .internal_pdelete(pattern, false, INTERNAL_CLIENT_ID, trace.clone())
            .await
        {
            debug!("Error in subscription monitoring: {e}");
        }

        if let Some(grave_goods) = grave_goods {
            info!(
                "Burying grave goods of client {client_id} ({}).",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );

            for grave_good in grave_goods {
                debug!(
                    "Deleting grave good key of client {client_id} ({}): {} ",
                    remote_addr
                        .map(|it| it.to_string())
                        .unwrap_or_else(|| "<unknown>".to_owned()),
                    grave_good
                );
                if let Err(e) = self
                    .internal_pdelete(
                        grave_good,
                        false,
                        client_id,
                        Trace::InternalAction(InternalAction::ApplyingGraveGoods {
                            cause: Box::new(trace.clone()),
                        }),
                    )
                    .await
                {
                    error!("Error burying grave goods for client {client_id}: {e}");
                }
            }
        } else {
            debug!(
                "Client {client_id} ({}) has no grave goods.",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );
        }

        if let Some(last_wills) = last_wills {
            info!(
                "Publishing last will of client {client_id} ({}).",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );

            for last_will in last_wills {
                debug!(
                    "Setting last will of client {client_id} ({}): {} = {}",
                    remote_addr
                        .map(|it| it.to_string())
                        .unwrap_or_else(|| "<unknown>".to_owned()),
                    last_will.key,
                    last_will.value
                );
                if let Err(e) = self
                    .internal_set(
                        last_will.key,
                        last_will.value,
                        client_id,
                        Trace::InternalAction(InternalAction::ApplyingLastWill {
                            cause: Box::new(trace.clone()),
                        }),
                        true,
                    )
                    .await
                {
                    error!("Error setting last will of client {client_id}: {e}");
                }
            }
        } else {
            debug!(
                "Client {client_id} ({}) has no last will.",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );
        }

        self.persistent_storage
            .remove_grave_goods_and_last_will(client_id)
            .await
            .map_err(|e| {
                WorterbuchError::IoError(
                    io::Error::other(e),
                    "Failed to remove grave goods and last will".to_owned(),
                )
            })?;

        if self.config.extended_monitoring
            && let Err(e) = self
                .internal_set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                    json!(self.subscriptions.len()),
                    INTERNAL_CLIENT_ID,
                    trace,
                    true,
                )
                .await
        {
            debug!("Error in subscription monitoring: {e}");
        }

        #[cfg(not(feature = "jemalloc"))]
        mem_tools::schedule_trim();

        Ok(())
    }

    fn store_key(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        key: Key,
        interface: Interface,
    ) {
        let keys = match self.spub_keys.entry(client_id) {
            Entry::Occupied(it) => it.into_mut(),
            Entry::Vacant(it) => it.insert(HashMap::new()),
        };
        keys.insert(transaction_id, (key, interface));
    }

    fn lookup_key(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> Option<(Key, Interface)> {
        self.spub_keys
            .get(&client_id)
            .and_then(|keys| keys.get(&transaction_id))
            .map(|(key, interface)| (key.to_owned(), interface.to_owned()))
    }

    pub(crate) async fn reset_store(&mut self, data: StoreNode) -> WorterbuchResult<()> {
        self.persistent_storage.clear().await.map_err(|e| {
            WorterbuchError::IoError(io::Error::other(e), "Failed to clear storage".to_owned())
        })?;

        let mut stack: Vec<(&StoreNode, Vec<&RegularKeySegment>)> = vec![(&data, vec![])];
        while let Some((node, path)) = stack.pop() {
            if let Some(value) = node.value() {
                let key = path
                    .iter()
                    .map(|s| s.to_owned().to_owned())
                    .reduce(|a, b| format!("{a}/{b}"))
                    .unwrap_or_else(|| "".to_owned());
                self.persistent_storage
                    .update_value(&key, value, None)
                    .await
                    .map_err(|e| {
                        WorterbuchError::IoError(
                            io::Error::other(e),
                            "Failed to insert value into persistent storage".to_owned(),
                        )
                    })?;
            }
            if let Some(children) = &node.tree {
                for (key, child) in children.iter() {
                    let mut child_path = path.clone();
                    child_path.push(key);
                    stack.push((child, child_path));
                }
            }
        }

        self.store.reset(data);

        Ok(())
    }

    pub(crate) async fn apply_all_grave_goods_and_last_wills(&mut self, cause: Trace) {
        self.apply_grave_goods(self.grave_goods(), cause.clone())
            .await;
        self.apply_last_wills(self.last_wills(), cause).await;
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    fn grave_goods(&self) -> GraveGoods {
        let pattern = topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_CLIENTS,
            KeySegment::Wildcard,
            SYSTEM_TOPIC_GRAVE_GOODS
        );

        let mut ggs = vec![];

        if let Ok(grave_goods) = self.pget(&pattern) {
            for KeyValuePair { key: _, value } in grave_goods {
                if let Ok(keys) = serde_json::from_value::<Vec<String>>(value) {
                    for key in keys {
                        ggs.push(key);
                    }
                }
            }
        }

        ggs
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    fn last_wills(&self) -> LastWill {
        let pattern = topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_CLIENTS,
            KeySegment::Wildcard,
            SYSTEM_TOPIC_LAST_WILL
        );

        let mut lws = vec![];

        if let Ok(last_will) = self.pget(&pattern) {
            for KeyValuePair { key: _, value } in last_will {
                if let Ok(kvps) = serde_json::from_value::<Vec<KeyValuePair>>(value) {
                    for kvp in kvps {
                        lws.push(kvp);
                    }
                }
            }
        }

        lws
    }

    #[instrument(skip(self))]
    pub(crate) async fn apply_grave_goods(&mut self, grave_goods: GraveGoods, cause: Trace) {
        for gg in grave_goods {
            self.internal_pdelete(
                gg,
                false,
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::ApplyingGraveGoods {
                    cause: Box::new(cause.clone()),
                }),
            )
            .await
            .ok();
        }
    }

    #[instrument(skip(self))]
    pub(crate) async fn apply_last_wills(&mut self, last_wills: LastWill, cause: Trace) {
        for lw in last_wills {
            self.internal_set(
                lw.key,
                lw.value,
                INTERNAL_CLIENT_ID,
                Trace::InternalAction(InternalAction::ApplyingLastWill {
                    cause: Box::new(cause.clone()),
                }),
                true,
            )
            .await
            .ok();
        }
    }

    pub(crate) fn set_persistent_storage(&mut self, persistent_storage: PersistentStorageImpl) {
        self.persistent_storage = persistent_storage;
    }

    pub(crate) async fn flush(&mut self) -> PersistenceResult<()> {
        let mut storage = mem::replace(&mut self.persistent_storage, PersistentStorageImpl::Noop);
        let res = storage.flush(self).await;
        _ = mem::replace(&mut self.persistent_storage, storage);
        res
    }
}

fn check_for_read_only_key(key: &str, client_id: ClientId) -> WorterbuchResult<()> {
    if key.is_empty() {
        return Err(WorterbuchError::EmptyKey);
    }

    if client_id == INTERNAL_CLIENT_ID {
        // modification is made internally by the server, so everything is allowed
        return Ok(());
    }

    let path: Vec<&str> = key.split('/').collect();

    if path.is_empty() || path[0] != SYSTEM_TOPIC_ROOT {
        // path is outside the protected $SYS prefix
        return Ok(());
    }

    if path.len() <= 3 || path[1] != SYSTEM_TOPIC_CLIENTS || path[2] != client_id.to_string() {
        // the only writable values are under $SYS/clients/[client_id]]/#
        return Err(WorterbuchError::ReadOnlyKey(key.to_owned()));
    }

    if path[3] == SYSTEM_TOPIC_GRAVE_GOODS
        || path[3] == SYSTEM_TOPIC_LAST_WILL
        || path[3] == SYSTEM_TOPIC_CLIENT_NAME
    {
        // clients may modify their last will or grave goods at any time
        return Ok(());
    }

    // TODO potentially whitelist more fields clients may change

    Err(WorterbuchError::ReadOnlyKey(key.to_owned()))
}

fn escape_wildcards(pattern: &str) -> String {
    pattern.replace('#', "%23").replace('?', "%3F")
}

#[cfg(test)]
mod test {

    #![allow(clippy::as_conversions)]
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn export_removes_system_keys() {
        dotenvy::dotenv().ok();
        let mut wb = Worterbuch::with_config(Config::new(None).await.unwrap());
        wb.set(
            "hello/world".to_owned(),
            json!("test"),
            false,
            TraceData::new(INTERNAL_CLIENT_ID, Interface::Local, 123),
        )
        .await
        .unwrap();
        wb.set(
            "$SYS/something".to_owned(),
            json!("this should not be exported"),
            false,
            TraceData::new(INTERNAL_CLIENT_ID, Interface::Protocol(Protocol::HTTP), 321),
        )
        .await
        .unwrap();

        let export = wb.export();
        assert_eq!(
            r#"[{"t":{"hello":{"t":{"world":{"v":"test"}}}}},[],[]]"#,
            &serde_json::to_string(&export).unwrap()
        );
    }
}
