use crate::{
    config::Config,
    stats::{
        SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_CLIENTS_ADDRESS, SYSTEM_TOPIC_CLIENTS_PROTOCOL,
        SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_ROOT,
        SYSTEM_TOPIC_SUBSCRIPTIONS,
    },
    store::{Store, StoreStats},
    subscribers::{LsSubscriber, Subscriber, Subscribers, SubscriptionId},
    INTERNAL_CLIENT_ID,
};
use hashlink::LinkedHashMap;
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json, to_value, Value};
use std::{collections::HashMap, fmt::Display, net::SocketAddr, ops::Deref, time::Duration};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    select, spawn,
    sync::mpsc::{self, unbounded_channel, UnboundedReceiver},
    time::sleep,
};
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    parse_segments, topic, GraveGoods, Key, KeySegment, KeyValuePairs, LastWill, PState,
    PStateEvent, Path, Protocol, ProtocolVersion, RegularKeySegment, RequestPattern, ServerMessage,
    TransactionId,
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
    async fn aggregate_loop(mut self, mut aggregate_rx: UnboundedReceiver<PStateEvent>) {
        let (send_trigger_tx, mut send_trigger_rx) = mpsc::channel::<()>(1);

        loop {
            select! {
                event = aggregate_rx.recv() => if let Some(event) = event {
                    if let Err(e) = self.aggregate(event, &send_trigger_tx).await {
                        log::error!("Error aggregating PState event: {e}");
                        break;
                    }
                } else {
                    break;
                },
                tick = send_trigger_rx.recv() => if let Some(_) = tick {
                    if let Err(e) = self.send_current_state().await {
                        log::error!("Error sending PState event: {e}");
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
    ) -> WorterbuchResult<()> {
        if !self.send_is_scheduled {
            self.schedule_send(send_trigger_tx.clone(), self.aggregate_duration);
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

    fn schedule_send(&mut self, send_trigger: mpsc::Sender<()>, aggregate_duration: Duration) {
        self.send_is_scheduled = true;
        spawn(async move {
            sleep(aggregate_duration).await;
            if let Err(e) = send_trigger.send(()).await {
                log::error!("Error triggering send of aggregated PState: {e}");
            }
        });
    }
}

pub struct PStateAggregator {
    aggregate: mpsc::UnboundedSender<PStateEvent>,
}

impl PStateAggregator {
    pub fn new(
        client_sub: mpsc::Sender<ServerMessage>,
        request_pattern: RequestPattern,
        aggregate_duration: Duration,
        transaction_id: TransactionId,
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

        let (aggregate_tx, aggregate_rx) = mpsc::unbounded_channel();

        spawn(aggregator_state.aggregate_loop(aggregate_rx));

        PStateAggregator {
            aggregate: aggregate_tx,
        }
    }

    pub async fn aggregate(&self, event: PStateEvent) -> WorterbuchResult<()> {
        self.aggregate.send(event)?;
        Ok(())
    }
}

#[derive(Default)]
pub struct Worterbuch {
    config: Config,
    store: Store,
    subscriptions: Subscriptions,
    ls_subscriptions: LsSubscriptions,
    subscribers: Subscribers,
    clients: HashMap<Uuid, SocketAddr>,
}

impl Worterbuch {
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn with_config(config: Config) -> Worterbuch {
        Worterbuch {
            config,
            ..Default::default()
        }
    }

    pub fn from_json(json: &str, config: Config) -> WorterbuchResult<Worterbuch> {
        let mut store: Store = from_str(json).context(|| format!("Error parsing JSON"))?;
        store.count_entries();
        Ok(Worterbuch {
            config,
            store,
            ..Default::default()
        })
    }

    pub fn len(&self) -> usize {
        self.store.len()
    }

    pub fn supported_protocol_version(&self) -> ProtocolVersion {
        "0.7".to_owned()
    }

    pub fn get(&self, key: &Key) -> WorterbuchResult<(String, Value)> {
        let path: Vec<RegularKeySegment> = parse_segments(key)?;

        match self.store.get(&path) {
            Some(value) => {
                let key_value = (key.to_owned(), value.to_owned());
                Ok(key_value)
            }
            None => Err(WorterbuchError::NoSuchValue(key.to_owned())),
        }
    }

    pub fn set(&mut self, key: Key, value: Value, client_id: &str) -> WorterbuchResult<()> {
        self.check_for_read_only_key(&key, client_id)?;

        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        let (changed, ls_subscribers) = self
            .store
            .insert(&path, value.clone())
            .map_err(|e| e.for_pattern(key.clone()))?;

        self.notify_ls_subscribers(ls_subscribers);
        self.notify_subscribers(path, key, value, changed, false);

        Ok(())
    }

    pub fn publish(&mut self, key: Key, value: Value) -> WorterbuchResult<()> {
        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        self.notify_subscribers(path, key, value, true, false);

        Ok(())
    }

    pub fn pget<'a>(&self, pattern: &str) -> WorterbuchResult<KeyValuePairs> {
        let path: Vec<KeySegment> = KeySegment::parse(pattern);
        self.store
            .get_matches(&path)
            .map_err(|e| e.for_pattern(pattern.to_owned()))
    }

    pub fn subscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)> {
        let path: Vec<KeySegment> = KeySegment::parse(&key);
        let (tx, rx) = unbounded_channel();
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(subscription.clone(), path.clone(), tx.clone(), unique);
        self.subscribers.add_subscriber(&path, subscriber);
        if !live_only {
            let matches = match self.get(&key) {
                Ok((key, value)) => Some((key, value)),
                Err(WorterbuchError::NoSuchValue(_)) => None,
                Err(e) => return Err(e),
            };
            if let Some((key, value)) = matches {
                tx.send(PStateEvent::KeyValuePairs(vec![(key, value).into()]))
                    .expect("rx is neither closed nor dropped");
            }
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());

        if self.config.extended_monitoring && &key != "$SYS" && !key.starts_with("$SYS/") {
            if let Err(e) = self.set(
                topic!("$SYS", "subscriptions"),
                json!(self.subscriptions.len()),
                INTERNAL_CLIENT_ID,
            ) {
                log::warn!("Error in subscription monitoring: {e}");
            }

            let subs_key = topic!("$SYS", "clients", client_id, "subscriptions");
            if let Err(e) = self.set(
                topic!(subs_key, key),
                json!(transaction_id),
                INTERNAL_CLIENT_ID,
            ) {
                log::warn!("Error in subscription monitoring: {e}");
            }

            let subs = self.ls(&Some(subs_key))?.len();
            self.set(
                topic!("$SYS", "clients", client_id, "subscriptions"),
                json!(subs),
                INTERNAL_CLIENT_ID,
            )?;
        }

        Ok((rx, subscription))
    }

    pub fn psubscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)> {
        let path: Vec<KeySegment> = KeySegment::parse(&pattern);
        let (tx, rx) = unbounded_channel();
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx.clone(),
            unique,
        );
        self.subscribers.add_subscriber(&path, subscriber);
        if !live_only {
            let matches = self.pget(&pattern)?;
            tx.send(PStateEvent::KeyValuePairs(matches))
                .expect("rx is neither closed nor dropped");
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());

        if self.config.extended_monitoring
            && &pattern != "#"
            && &pattern != "$SYS"
            && !pattern.starts_with("$SYS/")
        {
            if let Err(e) = self.set(
                topic!("$SYS", "subscriptions"),
                json!(self.subscriptions.len()),
                INTERNAL_CLIENT_ID,
            ) {
                log::warn!("Error in subscription monitoring: {e}");
            }
            let subs_key = topic!("$SYS", "clients", client_id, "subscriptions");
            if let Err(e) = self.set(
                topic!(subs_key, pattern),
                json!(transaction_id),
                INTERNAL_CLIENT_ID,
            ) {
                log::warn!("Error in subscription monitoring: {e}");
            }

            if let Err(e) = self
                .ls(&Some(subs_key))
                .map(|ls| ls.len())
                .and_then(|subs| {
                    self.set(
                        topic!("$SYS", "clients", client_id, "subscriptions"),
                        json!(subs),
                        INTERNAL_CLIENT_ID,
                    )
                })
            {
                log::warn!("Error in subscription monitoring: {e}");
            }
        }

        Ok((rx, subscription))
    }

    pub fn subscribe_ls(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        parent: Option<Key>,
    ) -> WorterbuchResult<(UnboundedReceiver<Vec<RegularKeySegment>>, SubscriptionId)> {
        let children = self.ls(&parent).unwrap_or_else(|_| Vec::new());
        let path: Vec<RegularKeySegment> = parent
            .map(|p| p.split("/").map(ToOwned::to_owned).collect())
            .unwrap_or_else(|| Vec::new());
        let (tx, rx) = unbounded_channel();
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = LsSubscriber::new(subscription.clone(), path.clone(), tx.clone());
        self.store.add_ls_subscriber(&path, subscriber);
        tx.send(children).expect("rx is neither closed nor dropped");
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.ls_subscriptions.insert(subscription_id, path);
        log::debug!("Total ls subscriptions: {}", self.ls_subscriptions.len());
        Ok((rx, subscription))
    }

    pub fn export(&self) -> WorterbuchResult<Value> {
        let value = to_value(&self.store)
            .context(|| format!("Error generating JSON from worterbuch store during export"))?;
        Ok(value)
    }

    pub fn import(&mut self, json: &str) -> WorterbuchResult<Vec<(String, Value)>> {
        log::debug!("Parsing store data …");
        let store: Store =
            from_str(json).context(|| format!("Error parsing JSON during import"))?;
        log::debug!("Done. Merging nodes …");
        let imported_values = self.store.merge(store);

        for (key, val) in &imported_values {
            let path: Vec<RegularKeySegment> = parse_segments(&key)?;
            self.notify_subscribers(
                path,
                key.to_owned(),
                val.to_owned(),
                // TODO only pass true if the value actually changed
                true,
                false,
            );
        }

        Ok(imported_values)
    }

    pub async fn export_to_file(&self, file: &mut File) -> WorterbuchResult<()> {
        log::debug!("Exporting to {file:?} …");
        let json = self.export()?.to_string();
        let json_bytes = json.as_bytes();

        file.write_all(json_bytes)
            .await
            .context(|| format!("Error writing to file {file:?}"))?;
        log::debug!("Done.");
        Ok(())
    }

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
        self.import(&json)?;
        log::info!("Done.");
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let subscription = SubscriptionId::new(client_id, transaction_id);
        self.do_unsubscribe(&subscription, client_id)
    }

    fn do_unsubscribe(
        &mut self,
        subscription: &SubscriptionId,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        if let Some(path) = self.subscriptions.remove(&subscription) {
            if self.config.extended_monitoring
                && path[0] != KeySegment::MultiWildcard
                && path[0].deref() != "$SYS"
            {
                if let Err(e) = self.delete(
                    topic!(
                        "$SYS",
                        "clients",
                        client_id,
                        "subscriptions",
                        path.iter()
                            .map(ToString::to_string)
                            .collect::<Vec<String>>()
                            .join("/")
                    ),
                    INTERNAL_CLIENT_ID,
                ) {
                    match e {
                        WorterbuchError::NoSuchValue(_) => (/* will happen on disconnect */),
                        _ => log::warn!("Error in subscription monitoring: {e}"),
                    }
                }
            }
            log::debug!("Remaining subscriptions: {}", self.subscriptions.len());

            if self.config.extended_monitoring {
                if let Err(e) = self.set(
                    topic!("$SYS", "subscriptions"),
                    json!(self.subscriptions.len()),
                    INTERNAL_CLIENT_ID,
                ) {
                    log::warn!("Error in subscription monitoring: {e}");
                }
            }
            if self.subscribers.unsubscribe(&path, &subscription) {
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
        if let Some(path) = self.ls_subscriptions.remove(&subscription) {
            log::debug!(
                "Remaining ls subscriptions: {}",
                self.ls_subscriptions.len()
            );
            if self.store.unsubscribe_ls(&path, &subscription) {
                Ok(())
            } else {
                Err(WorterbuchError::NotSubscribed)
            }
        } else {
            Err(WorterbuchError::NotSubscribed)
        }
    }

    fn notify_subscribers(
        &mut self,
        path: Vec<RegularKeySegment>,
        key: Key,
        value: Value,
        value_changed: bool,
        deleted: bool,
    ) {
        let subscribers = self.subscribers.get_subscribers(&path);

        let filtered_subscribers: Vec<Subscriber> = subscribers
            .into_iter()
            .filter(|s| value_changed || !s.is_unique())
            .collect();

        log::trace!(
            "Calling {} subscribers: {} = {:?}",
            filtered_subscribers.len(),
            key,
            value
        );
        for subscriber in filtered_subscribers {
            let kvps = vec![(key.clone(), value.clone()).into()];
            if let Err(e) = if deleted {
                subscriber.send(PStateEvent::Deleted(kvps))
            } else {
                subscriber.send(PStateEvent::KeyValuePairs(kvps))
            } {
                log::debug!("Error calling subscriber: {e}");
                self.subscribers.remove_subscriber(subscriber)
            }
        }
    }

    fn notify_ls_subscribers(&mut self, ls_subscribers: Vec<(Vec<LsSubscriber>, Vec<String>)>) {
        for (subscribers, new_children) in ls_subscribers {
            for subscriber in subscribers {
                if let Err(e) = subscriber.send(new_children.clone()) {
                    log::debug!("Error calling subscriber: {e}");
                    self.store.remove_ls_subscriber(subscriber)
                }
            }
        }
    }

    fn check_for_read_only_key(&self, key: &str, client_id: &str) -> WorterbuchResult<()> {
        if client_id == INTERNAL_CLIENT_ID {
            // modification is made internally by the server, so everything is allowed
            return Ok(());
        }

        let path: Vec<&str> = key.split('/').collect();

        if path.is_empty() || path[0] != SYSTEM_TOPIC_ROOT {
            // path is outside the protected $SYS prefix
            return Ok(());
        }

        if path.len() <= 3 || path[1] != SYSTEM_TOPIC_CLIENTS || path[2] != client_id {
            // the only writable values are under $SYS/clients/[client_id]]/#
            return Err(WorterbuchError::ReadOnlyKey(key.to_owned()));
        }

        if path[3] == SYSTEM_TOPIC_GRAVE_GOODS || path[3] == SYSTEM_TOPIC_LAST_WILL {
            // clients may modify their last will or grave goods at any time
            return Ok(());
        }

        // TODO potentially whitelist more fields clients may change

        return Err(WorterbuchError::ReadOnlyKey(key.to_owned()));
    }

    pub fn delete(&mut self, key: Key, client_id: &str) -> WorterbuchResult<(String, Value)> {
        self.check_for_read_only_key(&key, client_id)?;

        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        match self.store.delete(&path) {
            Some((value, ls_subscribers)) => {
                let key_value = (key.to_owned(), value.to_owned());
                self.notify_ls_subscribers(ls_subscribers);
                self.notify_subscribers(path, key.clone(), value.clone(), true, true);
                Ok(key_value)
            }
            None => Err(WorterbuchError::NoSuchValue(key)),
        }
    }

    pub fn pdelete(
        &mut self,
        pattern: RequestPattern,
        client_id: &str,
    ) -> WorterbuchResult<KeyValuePairs> {
        self.internal_pdelete(pattern, false, client_id)
    }

    fn internal_pdelete(
        &mut self,
        pattern: RequestPattern,
        skip_read_only_check: bool,
        client_id: &str,
    ) -> Result<Vec<worterbuch_common::KeyValuePair>, WorterbuchError> {
        if !skip_read_only_check {
            self.check_for_read_only_key(&pattern, client_id)?;
        }

        let path: Vec<KeySegment> = KeySegment::parse(&pattern);

        match self
            .store
            .delete_matches(&path)
            .map_err(|e| e.for_pattern(pattern.to_owned()))
        {
            Ok((deleted, ls_subscribers)) => {
                self.notify_ls_subscribers(ls_subscribers);
                for kvp in &deleted {
                    let path = parse_segments(&kvp.key)?;
                    self.notify_subscribers(path, kvp.key.clone(), kvp.value.clone(), true, true);
                }
                Ok(deleted)
            }
            Err(e) => Err(e),
        }
    }

    pub fn ls(&self, parent: &Option<Key>) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let path = parent
            .as_deref()
            .map(|p| p.split("/").collect())
            .unwrap_or_else(|| Vec::new());
        self.ls_path(&path)
    }

    fn ls_path<'s>(&self, path: &[&'s str]) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let children = if !path.is_empty() {
            self.store.ls(path)
        } else {
            Some(self.store.ls_root())
        };

        match children {
            Some(children) => Ok(children),
            None => Err(WorterbuchError::NoSuchValue(path.join("/"))),
        }
    }

    pub fn connected(&mut self, client_id: Uuid, remote_addr: SocketAddr, protocol: Protocol) {
        self.clients.insert(client_id, remote_addr);
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        if let Err(e) = self.set(
            client_count_key,
            json!(self.clients.len()),
            INTERNAL_CLIENT_ID,
        ) {
            log::error!("Error updating client count: {e}");
        }
        if let Err(e) = serde_json::to_value(&protocol)
            .map_err(|e| {
                WorterbuchError::SerDeError(e, format!("could not convert protocol to value"))
            })
            .and_then(|protocol| {
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
            })
        {
            log::error!("Error updating client protocol: {e}");
        }

        if let Err(e) = serde_json::to_value(&remote_addr)
            .map_err(|e| {
                WorterbuchError::SerDeError(e, format!("could not convert remote address to value"))
            })
            .and_then(|remote_addr| {
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
            })
        {
            log::error!("Error updating client address: {e}");
        }
    }

    fn grave_goods(&self, client_id: &Uuid) -> Option<GraveGoods> {
        let key = topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_CLIENTS,
            client_id,
            SYSTEM_TOPIC_GRAVE_GOODS
        );
        let value = self.get(&key).ok();
        value.and_then(|it| serde_json::from_value(it.1).ok())
    }

    fn last_wills(&self, client_id: &Uuid) -> Option<LastWill> {
        let key = topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_CLIENTS,
            client_id,
            SYSTEM_TOPIC_LAST_WILL
        );
        let value = self.get(&key).ok();
        value.and_then(|it| serde_json::from_value(it.1).ok())
    }

    pub fn disconnected(
        &mut self,
        client_id: Uuid,
        remote_addr: SocketAddr,
    ) -> WorterbuchResult<()> {
        let grave_goods = self.grave_goods(&client_id);
        let last_wills = self.last_wills(&client_id);

        let pattern = topic!("$SYS", "clients", client_id, "#");
        if self.config.extended_monitoring {
            log::debug!("Deleting {pattern}");
            if let Err(e) = self.pdelete(pattern, INTERNAL_CLIENT_ID) {
                log::warn!("Error in subscription monitoring: {e}");
            }
        }
        self.clients.remove(&client_id);
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        if let Err(e) = self.set(
            client_count_key,
            json!(self.clients.len()),
            INTERNAL_CLIENT_ID,
        ) {
            log::error!("Error updating client count: {e}");
        }

        let subscription_keys: Vec<SubscriptionId> = self
            .subscriptions
            .keys()
            .filter(|k| k.client_id == client_id)
            .map(ToOwned::to_owned)
            .collect();
        log::info!(
            "Removing {} subscription(s) of client {client_id} ({remote_addr}).",
            subscription_keys.len()
        );
        for subscription in subscription_keys {
            if let Err(e) = self.do_unsubscribe(&subscription, client_id) {
                log::error!("Inconsistent subscription state: {e}");
            }
        }

        if let Some(grave_goods) = grave_goods {
            log::info!("Burying grave goods of client {client_id} ({remote_addr}).");

            for grave_good in grave_goods {
                log::debug!(
                    "Deleting grave good key of client {client_id} ({remote_addr}): {} ",
                    grave_good
                );
                if let Err(e) = self.pdelete(grave_good, &client_id.to_string()) {
                    log::error!("Error burying grave goods for client {client_id}: {e}")
                }
            }
        } else {
            log::info!("Client {client_id} ({remote_addr}) has no grave goods.");
        }

        if let Some(last_wills) = last_wills {
            log::info!("Publishing last will of client {client_id} ({remote_addr}).");

            for last_will in last_wills {
                log::debug!(
                    "Setting last will of client {client_id} ({remote_addr}): {} = {}",
                    last_will.key,
                    last_will.value
                );
                if let Err(e) = self.set(last_will.key, last_will.value, &client_id.to_string()) {
                    log::error!("Error setting last will of client {client_id}: {e}")
                }
            }
        } else {
            log::info!("Client {client_id} ({remote_addr}) has no last will.");
        }

        if self.config.extended_monitoring {
            if let Err(e) = self.set(
                topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SUBSCRIPTIONS),
                json!(self.subscriptions.len()),
                INTERNAL_CLIENT_ID,
            ) {
                log::warn!("Error in subscription monitoring: {e}");
            }
        }

        if let Err(e) = self.delete(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_PROTOCOL
            ),
            INTERNAL_CLIENT_ID,
        ) {
            log::debug!("Error updating client protocol: {e}");
        }

        if let Err(e) = self.delete(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                client_id,
                SYSTEM_TOPIC_CLIENTS_ADDRESS
            ),
            INTERNAL_CLIENT_ID,
        ) {
            log::debug!("Error updating client address: {e}");
        }

        Ok(())
    }
}
