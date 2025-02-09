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

use crate::{
    config::Config,
    store::{Node, Store, StoreStats},
    subscribers::{EventSender, LsSubscriber, Subscriber, Subscribers, SubscriptionId},
    INTERNAL_CLIENT_ID,
};
use hashlink::LinkedHashMap;
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json, Value};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    net::SocketAddr,
    ops::Deref,
    time::Duration,
};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    select, spawn,
    sync::{
        mpsc::{self, channel, Receiver},
        oneshot,
    },
    time::sleep,
};
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    parse_segments, topic, GraveGoods, Key, KeySegment, KeyValuePairs, LastWill, PState,
    PStateEvent, Path, Protocol, ProtocolVersion, RegularKeySegment, RequestPattern, ServerMessage,
    StateEvent, TransactionId, PROTOCOL_VERSION, SYSTEM_TOPIC_CLIENTS,
    SYSTEM_TOPIC_CLIENTS_ADDRESS, SYSTEM_TOPIC_CLIENTS_PROTOCOL, SYSTEM_TOPIC_CLIENT_NAME,
    SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_ROOT_PREFIX,
    SYSTEM_TOPIC_SUBSCRIPTIONS,
};

pub type Subscriptions = HashMap<SubscriptionId, Vec<KeySegment>>;
pub type LsSubscriptions = HashMap<SubscriptionId, Vec<RegularKeySegment>>;

type Map<K, V> = LinkedHashMap<K, V>;

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Stats {
    store_stats: StoreStats,
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = serde_json::to_string(self).expect("serialization cannot fail");
        write!(f, "{str}")
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
    async fn aggregate_loop(mut self, mut aggregate_rx: Receiver<PStateEvent>, client_id: Uuid) {
        let (send_trigger_tx, mut send_trigger_rx) = mpsc::channel::<()>(1);

        loop {
            select! {
                event = aggregate_rx.recv() => if let Some(event) = event {
                    if let Err(e) = self.aggregate(event, &send_trigger_tx, client_id).await {
                        log::error!("Error aggregating PState event for client {client_id}: {e}");
                        break;
                    }
                } else {
                    break;
                },
                tick = send_trigger_rx.recv() => if tick.is_some() {
                    if let Err(e) = self.send_current_state().await {
                        log::error!("Error sending PState event to client {client_id}: {e}");
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
        client_id: Uuid,
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
        client_id: Uuid,
    ) {
        self.send_is_scheduled = true;
        spawn(async move {
            sleep(aggregate_duration).await;
            if let Err(e) = send_trigger.send(()).await {
                log::error!(
                    "Error triggering send of aggregated PState to client {client_id}: {e}"
                );
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
        client_id: Uuid,
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
    clients: HashMap<Uuid, Option<SocketAddr>>,
    spub_keys: HashMap<Uuid, HashMap<TransactionId, Key>>,
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
        }
    }

    pub fn from_json(json: &str, config: Config) -> WorterbuchResult<Worterbuch> {
        let mut store: Store = from_str(json).context(|| "Error parsing JSON".to_owned())?;
        store.count_entries();
        Ok(Worterbuch {
            config,
            store,
            clients: Default::default(),
            ls_subscriptions: Default::default(),
            subscribers: Default::default(),
            subscriptions: Default::default(),
            spub_keys: Default::default(),
        })
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }

    pub fn supported_protocol_version(&self) -> ProtocolVersion {
        PROTOCOL_VERSION.to_owned()
    }

    pub fn get(&self, key: &Key) -> WorterbuchResult<Value> {
        let path: Vec<RegularKeySegment> = parse_segments(key)?;

        match self.store.get(&path) {
            Some(value) => Ok(value.to_owned()),
            None => Err(WorterbuchError::NoSuchValue(key.to_owned())),
        }
    }

    pub async fn set(&mut self, key: Key, value: Value, client_id: Uuid) -> WorterbuchResult<()> {
        check_for_read_only_key(&key, client_id)?;

        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        let (changed, ls_subscribers) = self
            .store
            .insert(&path, value.clone())
            .map_err(|e| e.for_pattern(key.clone()))?;

        log::trace!("Notifying ls subscribers …");
        self.notify_ls_subscribers(ls_subscribers).await;
        log::trace!("Notifying ls subscribers done.");
        log::trace!("Notifying subscribers …");
        self.notify_subscribers(&path, &key, &value, changed, false)
            .await;
        log::trace!("Notifying subscribers done.");

        Ok(())
    }

    pub async fn spub_init(
        &mut self,
        transaction_id: TransactionId,
        key: Key,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        check_for_read_only_key(&key, client_id)?;
        self.store_key(client_id, transaction_id, key);

        Ok(())
    }

    pub async fn spub(
        &mut self,
        transaction_id: TransactionId,
        value: Value,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        if let Some(key) = self.lookup_key(client_id, transaction_id) {
            self.publish(key, value).await
        } else {
            Err(WorterbuchError::NoPubStream(transaction_id))
        }
    }

    pub async fn publish(&mut self, key: Key, value: Value) -> WorterbuchResult<()> {
        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        self.notify_subscribers(&path, &key, &value, true, false)
            .await;

        Ok(())
    }

    pub fn pget(&self, pattern: &str) -> WorterbuchResult<KeyValuePairs> {
        let path: Vec<KeySegment> = KeySegment::parse(pattern);
        self.store
            .get_matches(&path)
            .map_err(|e| e.for_pattern(pattern.to_owned()))
    }

    pub async fn subscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(Receiver<StateEvent>, SubscriptionId)> {
        let path: Vec<KeySegment> = KeySegment::parse(&key);
        let (tx, rx) = channel::<StateEvent>(self.config.channel_buffer_size);
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone(),
            EventSender::State(tx.clone()),
            unique,
        );
        self.subscribers.add_subscriber(&path, subscriber);
        if !live_only {
            let matches = match self.get(&key) {
                Ok(value) => Some(value),
                Err(WorterbuchError::NoSuchValue(_)) => None,
                Err(e) => return Err(e),
            };
            if let Some(value) = matches {
                tx.send(StateEvent::Value(value))
                    .await
                    .expect("rx is neither closed nor dropped");
            }
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());

        if self.config.extended_monitoring
            && key != SYSTEM_TOPIC_ROOT
            && !key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX)
        {
            if let Err(e) = self
                .set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                    json!(self.subscriptions.len()),
                    INTERNAL_CLIENT_ID,
                )
                .await
            {
                log::warn!("Error in subscription monitoring: {e}");
            }

            let subs_key = topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_SUBSCRIPTIONS
            );
            if let Err(e) = self
                .set(
                    topic!(subs_key, key),
                    json!(transaction_id),
                    INTERNAL_CLIENT_ID,
                )
                .await
            {
                log::warn!("Error in subscription monitoring: {e}");
            }

            let subs = self.ls(&Some(subs_key))?.len();
            self.set(
                topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_SUBSCRIPTIONS
                ),
                json!(subs),
                INTERNAL_CLIENT_ID,
            )
            .await?;
        }

        Ok((rx, subscription))
    }

    pub async fn psubscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(Receiver<PStateEvent>, SubscriptionId)> {
        let path: Vec<KeySegment> = KeySegment::parse(&pattern);
        let (tx, rx) = channel(self.config.channel_buffer_size);
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone().into_iter().map(|s| s.to_owned()).collect(),
            EventSender::PState(tx.clone()),
            unique,
        );
        self.subscribers.add_subscriber(&path, subscriber);
        if !live_only {
            let matches = self.pget(&pattern)?;
            tx.send(PStateEvent::KeyValuePairs(matches))
                .await
                .expect("rx is neither closed nor dropped");
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());

        if self.config.extended_monitoring
            && pattern != "#"
            && pattern != SYSTEM_TOPIC_ROOT
            && !pattern.starts_with(SYSTEM_TOPIC_ROOT_PREFIX)
        {
            if let Err(e) = self
                .set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                    json!(self.subscriptions.len()),
                    INTERNAL_CLIENT_ID,
                )
                .await
            {
                log::warn!("Error in subscription monitoring: {e}");
            }
            let subs_key = topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_SUBSCRIPTIONS
            );
            if let Err(e) = self
                .set(
                    topic!(subs_key, escape_wildcards(&pattern)),
                    json!(transaction_id),
                    INTERNAL_CLIENT_ID,
                )
                .await
            {
                log::warn!("Error in subscription monitoring: {e}");
            }
            if let Err(e) = self.update_subscription_count(client_id, &subs_key).await {
                log::warn!("Error in subscription monitoring: {e}");
            }
        }

        Ok((rx, subscription))
    }

    async fn update_subscription_count(
        &mut self,
        client_id: Uuid,
        subs_key: &str,
    ) -> WorterbuchResult<()> {
        let subs = self.sub_len(subs_key)?.unwrap_or(0);
        self.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_SUBSCRIPTIONS
            ),
            json!(subs),
            INTERNAL_CLIENT_ID,
        )
        .await
    }

    pub async fn subscribe_ls(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        parent: Option<Key>,
    ) -> WorterbuchResult<(Receiver<Vec<RegularKeySegment>>, SubscriptionId)> {
        let children = self.ls(&parent).unwrap_or_else(|_| Vec::new());
        let path: Vec<RegularKeySegment> = parent
            .map(|p| p.split('/').map(ToOwned::to_owned).collect())
            .unwrap_or_default();
        let (tx, rx) = channel(self.config.channel_buffer_size);
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = LsSubscriber::new(subscription.clone(), path.clone(), tx.clone());
        self.store.add_ls_subscriber(&path, subscriber);
        tx.send(children)
            .await
            .expect("rx is neither closed nor dropped");
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.ls_subscriptions.insert(subscription_id, path);
        log::debug!("Total ls subscriptions: {}", self.ls_subscriptions.len());
        Ok((rx, subscription))
    }

    pub fn export(&mut self) -> Node {
        self.store.export()
    }

    pub fn export_for_persistence(&mut self, tx: oneshot::Sender<Option<Value>>) {
        if !self.store.has_unsaved_changes() {
            tx.send(None).ok();
            return;
        }
        let store = self.store.export_for_persistence();
        spawn(async move {
            let value = json!(store);
            tx.send(Some(value)).ok();
        });
    }

    pub async fn import(&mut self, json: &str) -> WorterbuchResult<Vec<(String, Value)>> {
        log::debug!("Parsing store data …");
        let store: Store =
            from_str(json).context(|| "Error parsing JSON during import".to_owned())?;
        log::debug!("Done. Merging nodes …");
        let imported_values = self.store.merge(store);

        for (key, val) in &imported_values {
            let path: Vec<RegularKeySegment> = parse_segments(key)?;
            self.notify_subscribers(
                &path, key, val, // TODO only pass true if the value actually changed
                true, false,
            )
            .await;
        }

        Ok(imported_values)
    }

    // pub async fn export_to_file(&mut self, file: &mut File) -> WorterbuchResult<()> {
    //     log::debug!("Exporting to {file:?} …");

    //     let (tx, rx) = oneshot::channel();
    //     self.export(tx);
    //     let json = rx.await?.to_string();
    //     let json_bytes = json.as_bytes();

    //     file.write_all(json_bytes)
    //         .await
    //         .context(|| format!("Error writing to file {file:?}"))?;
    //     log::debug!("Done.");
    //     Ok(())
    // }

    pub async fn import_from_file(&mut self, path: &Path) -> WorterbuchResult<()> {
        log::info!("Importing from {path} …");
        let mut file = File::open(path)
            .await
            .context(|| format!("Error opening file {path:?}"))?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)
            .await
            .context(|| format!("Error reading file {path}"))?;
        let json = String::from_utf8_lossy(&contents).to_string();
        self.import(&json).await?;
        log::info!("Done.");
        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let subscription = SubscriptionId::new(client_id, transaction_id);
        self.do_unsubscribe(&subscription, client_id).await
    }

    async fn do_unsubscribe(
        &mut self,
        subscription: &SubscriptionId,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        if let Some(path) = self.subscriptions.remove(subscription) {
            if self.config.extended_monitoring
                && path[0] != KeySegment::MultiWildcard
                && path[0].deref() != SYSTEM_TOPIC_ROOT
            {
                if let Err(e) = self
                    .delete(
                        topic!(
                            SYSTEM_TOPIC_ROOT,
                            SYSTEM_TOPIC_CLIENTS,
                            client_id,
                            SYSTEM_TOPIC_SUBSCRIPTIONS,
                            escape_wildcards(
                                &path
                                    .iter()
                                    .map(ToString::to_string)
                                    .collect::<Vec<String>>()
                                    .join("/")
                            )
                        ),
                        INTERNAL_CLIENT_ID,
                    )
                    .await
                {
                    match e {
                        WorterbuchError::NoSuchValue(_) => (/* will happen on disconnect */),
                        _ => log::warn!("Error in subscription monitoring: {e}"),
                    }
                }
            }
            log::debug!("Remaining subscriptions: {}", self.subscriptions.len());

            if self.config.extended_monitoring {
                if let Err(e) = self
                    .set(
                        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                        json!(self.subscriptions.len()),
                        INTERNAL_CLIENT_ID,
                    )
                    .await
                {
                    log::warn!("Error in subscription monitoring: {e}");
                }
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
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let subscription = SubscriptionId::new(client_id, transaction_id);
        self.do_unsubscribe_ls(&subscription)
    }

    fn do_unsubscribe_ls(&mut self, subscription: &SubscriptionId) -> WorterbuchResult<()> {
        if let Some(path) = self.ls_subscriptions.remove(subscription) {
            log::debug!(
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
    ) {
        let subscribers = self.subscribers.get_subscribers(path);

        let filtered_subscribers: Vec<Subscriber> = subscribers
            .into_iter()
            .filter(|s| value_changed || !s.is_unique())
            .collect();

        let len = filtered_subscribers.len();
        log::trace!("Calling {} subscribers: {} = {:?} …", len, key, value);
        for subscriber in filtered_subscribers {
            if subscriber.is_pstate_subscriber() {
                let kvps = vec![(key.clone(), value.clone()).into()];
                if let Err(e) = if deleted {
                    subscriber.send_pstate(PStateEvent::Deleted(kvps)).await
                } else {
                    subscriber
                        .send_pstate(PStateEvent::KeyValuePairs(kvps))
                        .await
                } {
                    log::debug!("Error calling subscriber: {e}");
                    self.subscribers.remove_subscriber(subscriber);
                }
            } else {
                let value = value.to_owned();
                if let Err(e) = if deleted {
                    subscriber.send_state(StateEvent::Deleted(value)).await
                } else {
                    subscriber.send_state(StateEvent::Value(value)).await
                } {
                    log::debug!("Error calling subscriber: {e}");
                    self.subscribers.remove_subscriber(subscriber);
                }
            }
        }
        log::trace!("Calling {} subscribers: {} = {:?} done.", len, key, value);
    }

    async fn notify_ls_subscribers(
        &mut self,
        ls_subscribers: Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) {
        let len = ls_subscribers.len();
        log::trace!("Calling {} ls subscribers …", len);
        for (subscribers, new_children) in ls_subscribers {
            for subscriber in subscribers {
                if let Err(e) = subscriber.send(new_children.clone()).await {
                    log::debug!("Error calling subscriber: {e}");
                    self.store.remove_ls_subscriber(subscriber);
                }
            }
        }
        log::trace!("Calling {} ls subscribers done.", len);
    }

    pub async fn delete(&mut self, key: Key, client_id: Uuid) -> WorterbuchResult<Value> {
        check_for_read_only_key(&key, client_id)?;

        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        match self.store.delete(&path) {
            Some((value, ls_subscribers)) => {
                self.notify_ls_subscribers(ls_subscribers).await;
                self.notify_subscribers(&path, &key, &value, true, true)
                    .await;
                Ok(value)
            }
            None => Err(WorterbuchError::NoSuchValue(key)),
        }
    }

    pub async fn pdelete(
        &mut self,
        pattern: RequestPattern,
        client_id: Uuid,
    ) -> WorterbuchResult<KeyValuePairs> {
        self.internal_pdelete(pattern, false, client_id).await
    }

    async fn internal_pdelete(
        &mut self,
        pattern: RequestPattern,
        skip_read_only_check: bool,
        client_id: Uuid,
    ) -> Result<Vec<worterbuch_common::KeyValuePair>, WorterbuchError> {
        if !skip_read_only_check {
            check_for_read_only_key(&pattern, client_id)?;
        }

        let path: Vec<KeySegment> = KeySegment::parse(&pattern);

        match self
            .store
            .delete_matches(&path)
            .map_err(|e| e.for_pattern(pattern))
        {
            Ok((deleted, ls_subscribers)) => {
                self.notify_ls_subscribers(ls_subscribers).await;
                for kvp in &deleted {
                    let path = parse_segments(&kvp.key)?;
                    self.notify_subscribers(&path, &kvp.key, &kvp.value, true, true)
                        .await;
                }
                Ok(deleted)
            }
            Err(e) => Err(e),
        }
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

    fn sub_len(&self, subkey: &str) -> WorterbuchResult<Option<usize>> {
        self.store.count_sub_entries(subkey)
    }

    pub async fn connected(
        &mut self,
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
        protocol: &Protocol,
    ) {
        self.clients.insert(client_id, remote_addr);
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        if let Err(e) = self
            .set(
                client_count_key,
                json!(self.clients.len()),
                INTERNAL_CLIENT_ID,
            )
            .await
        {
            log::error!("Error updating client count: {e}");
        }
        if let Err(e) = self.set_client_protocol(&client_id, protocol).await {
            log::error!("Error updating client protocol: {e}");
        };

        if let Err(e) = self.set_client_address(&client_id, remote_addr).await {
            log::error!("Error updating client address: {e}");
        }
    }

    async fn set_client_protocol(
        &mut self,
        client_id: &Uuid,
        protocol: &Protocol,
    ) -> WorterbuchResult<()> {
        let protocol = serde_json::to_value(protocol).map_err(|e| {
            WorterbuchError::SerDeError(e, "could not convert protocol to value".to_owned())
        })?;
        self.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_PROTOCOL
            ),
            protocol,
            INTERNAL_CLIENT_ID,
        )
        .await
    }

    async fn set_client_address(
        &mut self,
        client_id: &Uuid,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        let remote_addr = serde_json::to_value(remote_addr).map_err(|e| {
            WorterbuchError::SerDeError(e, "could not convert remote address to value".to_owned())
        })?;
        self.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_ADDRESS
            ),
            remote_addr,
            INTERNAL_CLIENT_ID,
        )
        .await
    }

    fn grave_goods(&self, client_id: &Uuid) -> Option<GraveGoods> {
        let key = topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_CLIENTS,
            client_id,
            SYSTEM_TOPIC_GRAVE_GOODS
        );
        let value = self.get(&key).ok();
        value.and_then(|it| serde_json::from_value(it).ok())
    }

    fn last_wills(&self, client_id: &Uuid) -> Option<LastWill> {
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
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        if let Some(spubs) = self.spub_keys.remove(&client_id) {
            log::info!(
                "Dropping {} pub stream(s) of client {}.",
                spubs.len(),
                client_id
            );
        }

        let grave_goods = self.grave_goods(&client_id);
        let last_wills = self.last_wills(&client_id);

        let pattern = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS, client_id, "#");
        if self.config.extended_monitoring {
            log::debug!("Deleting {pattern}");
            if let Err(e) = self.pdelete(pattern, INTERNAL_CLIENT_ID).await {
                log::warn!("Error in subscription monitoring: {e}");
            }
        }
        self.clients.remove(&client_id);
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        if let Err(e) = self
            .set(
                client_count_key,
                json!(self.clients.len()),
                INTERNAL_CLIENT_ID,
            )
            .await
        {
            log::error!("Error updating client count: {e}");
        }

        let subscription_keys: Vec<SubscriptionId> = self
            .subscriptions
            .keys()
            .filter(|k| k.client_id == client_id)
            .map(ToOwned::to_owned)
            .collect();
        log::info!(
            "Removing {} subscription(s) of client {client_id} ({}).",
            subscription_keys.len(),
            remote_addr
                .map(|it| it.to_string())
                .unwrap_or_else(|| "<unknown>".to_owned())
        );
        for subscription in subscription_keys {
            if let Err(e) = self.do_unsubscribe(&subscription, client_id).await {
                log::error!("Inconsistent subscription state: {e}");
            }
        }

        if let Some(grave_goods) = grave_goods {
            log::info!(
                "Burying grave goods of client {client_id} ({}).",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );

            for grave_good in grave_goods {
                log::debug!(
                    "Deleting grave good key of client {client_id} ({}): {} ",
                    remote_addr
                        .map(|it| it.to_string())
                        .unwrap_or_else(|| "<unknown>".to_owned()),
                    grave_good
                );
                if let Err(e) = self.pdelete(grave_good, client_id).await {
                    log::error!("Error burying grave goods for client {client_id}: {e}");
                }
            }
        } else {
            log::info!(
                "Client {client_id} ({}) has no grave goods.",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );
        }

        if let Some(last_wills) = last_wills {
            log::info!(
                "Publishing last will of client {client_id} ({}).",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );

            for last_will in last_wills {
                log::debug!(
                    "Setting last will of client {client_id} ({}): {} = {}",
                    remote_addr
                        .map(|it| it.to_string())
                        .unwrap_or_else(|| "<unknown>".to_owned()),
                    last_will.key,
                    last_will.value
                );
                if let Err(e) = self.set(last_will.key, last_will.value, client_id).await {
                    log::error!("Error setting last will of client {client_id}: {e}");
                }
            }
        } else {
            log::info!(
                "Client {client_id} ({}) has no last will.",
                remote_addr
                    .map(|it| it.to_string())
                    .unwrap_or_else(|| "<unknown>".to_owned())
            );
        }

        if self.config.extended_monitoring {
            if let Err(e) = self
                .set(
                    topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                    json!(self.subscriptions.len()),
                    INTERNAL_CLIENT_ID,
                )
                .await
            {
                log::warn!("Error in subscription monitoring: {e}");
            }
        }

        if let Err(e) = self
            .delete(
                topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_CLIENTS_PROTOCOL
                ),
                INTERNAL_CLIENT_ID,
            )
            .await
        {
            log::debug!("Error updating client protocol: {e}");
        }

        if let Err(e) = self
            .delete(
                topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_CLIENTS_ADDRESS
                ),
                INTERNAL_CLIENT_ID,
            )
            .await
        {
            log::debug!("Error updating client address: {e}");
        }

        Ok(())
    }

    fn store_key(&mut self, client_id: Uuid, transaction_id: u64, key: Key) {
        let keys = match self.spub_keys.entry(client_id) {
            Entry::Occupied(it) => it.into_mut(),
            Entry::Vacant(it) => it.insert(HashMap::new()),
        };
        keys.insert(transaction_id, key);
    }

    fn lookup_key(&self, client_id: Uuid, transaction_id: u64) -> Option<Key> {
        self.spub_keys
            .get(&client_id)
            .and_then(|keys| keys.get(&transaction_id))
            .map(ToOwned::to_owned)
    }

    pub(crate) fn reset_store(&mut self, data: Node) {
        self.store.reset(data);
    }
}

fn check_for_read_only_key(key: &str, client_id: Uuid) -> WorterbuchResult<()> {
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
    use super::*;

    #[tokio::test]
    async fn export_removes_system_keys() {
        dotenv::dotenv().ok();
        let mut wb = Worterbuch::with_config(Config::new().await.unwrap());
        wb.set("hello/world".to_owned(), json!("test"), INTERNAL_CLIENT_ID)
            .await
            .unwrap();
        wb.set(
            "$SYS/something".to_owned(),
            json!("this should not be exported"),
            INTERNAL_CLIENT_ID,
        )
        .await
        .unwrap();

        let export = wb.export();
        assert_eq!(
            r#"{"t":{"hello":{"t":{"world":{"v":"test"}}}}}"#,
            &serde_json::to_string(&export).unwrap()
        );
    }
}
