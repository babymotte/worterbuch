use crate::{
    config::Config,
    stats::{SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_ROOT},
    store::{Store, StoreStats},
    subscribers::{LsSubscriber, Subscriber, Subscribers, SubscriptionId},
};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, json, to_value, Value};
use std::{collections::HashMap, fmt::Display, net::SocketAddr};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
};
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    parse_segments, topic, GraveGoods, Handshake, Key, KeySegment, KeyValuePairs, LastWill,
    PStateEvent, Path, ProtocolVersion, ProtocolVersions, RegularKeySegment, RequestPattern,
    TransactionId,
};

pub type Subscriptions = HashMap<SubscriptionId, Vec<KeySegment>>;
pub type LsSubscriptions = HashMap<SubscriptionId, Vec<RegularKeySegment>>;

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

#[derive(Default)]
pub struct Worterbuch {
    config: Config,
    store: Store,
    subscriptions: Subscriptions,
    ls_subscriptions: LsSubscriptions,
    subscribers: Subscribers,
    last_wills: HashMap<Uuid, LastWill>,
    grave_goods: HashMap<Uuid, GraveGoods>,
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

    pub fn supported_protocol_versions(&self) -> ProtocolVersions {
        vec![ProtocolVersion { major: 0, minor: 6 }]
    }

    pub fn handshake(
        &mut self,
        client_protocol_versions: &ProtocolVersions,
        last_will: LastWill,
        grave_goods: GraveGoods,
        client_id: Uuid,
    ) -> WorterbuchResult<Handshake> {
        let mut supported_protocol_versions = self.supported_protocol_versions();

        supported_protocol_versions.retain(|e| client_protocol_versions.contains(e));
        supported_protocol_versions.sort();
        let protocol_version = match supported_protocol_versions.into_iter().last() {
            Some(version) => version,
            None => return Err(WorterbuchError::ProtocolNegotiationFailed),
        };

        if !last_will.is_empty() {
            self.last_wills.insert(client_id, last_will);
        }

        if !grave_goods.is_empty() {
            self.grave_goods.insert(client_id, grave_goods);
        }

        let handshake = Handshake { protocol_version };

        Ok(handshake)
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

    pub fn set(&mut self, key: Key, value: Value) -> WorterbuchResult<()> {
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
    ) -> WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)> {
        let path: Vec<KeySegment> = KeySegment::parse(&key);
        let matches = match self.get(&key) {
            Ok((key, value)) => Some((key, value)),
            Err(WorterbuchError::NoSuchValue(_)) => None,
            Err(e) => return Err(e),
        };
        let (tx, rx) = unbounded_channel();
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(subscription.clone(), path.clone(), tx.clone(), unique);
        self.subscribers.add_subscriber(&path, subscriber);
        if let Some((key, value)) = matches {
            tx.send(PStateEvent::KeyValuePairs(vec![(key, value).into()]))
                .expect("rx is neither closed nor dropped");
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());
        Ok((rx, subscription))
    }

    pub fn psubscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: bool,
    ) -> WorterbuchResult<(UnboundedReceiver<PStateEvent>, SubscriptionId)> {
        let path: Vec<KeySegment> = KeySegment::parse(&pattern);
        let matches = self.pget(&pattern)?;
        let (tx, rx) = unbounded_channel();
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx.clone(),
            unique,
        );
        self.subscribers.add_subscriber(&path, subscriber);
        tx.send(PStateEvent::KeyValuePairs(matches))
            .expect("rx is neither closed nor dropped");
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, path);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());
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
        self.do_unsubscribe(&subscription)
    }

    fn do_unsubscribe(&mut self, subscription: &SubscriptionId) -> WorterbuchResult<()> {
        if let Some(path) = self.subscriptions.remove(&subscription) {
            log::debug!("Remaining subscriptions: {}", self.subscriptions.len());
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

    pub fn delete(&mut self, key: Key) -> WorterbuchResult<(Key, Value)> {
        let path: Vec<RegularKeySegment> = parse_segments(&key)?;

        if path.is_empty() || path[0] == SYSTEM_TOPIC_ROOT {
            return Err(WorterbuchError::ReadOnlyKey(key));
        }

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

    pub fn pdelete(&mut self, pattern: RequestPattern) -> WorterbuchResult<KeyValuePairs> {
        let path: Vec<KeySegment> = KeySegment::parse(&pattern);

        if path.is_empty()
            || &*(path[0]) == SYSTEM_TOPIC_ROOT
            || path[0] == KeySegment::MultiWildcard
        {
            return Err(WorterbuchError::ReadOnlyKey(pattern));
        }

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

    pub fn connected(&mut self, client_id: Uuid, remote_addr: SocketAddr) {
        self.clients.insert(client_id, remote_addr);
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        if let Err(e) = self.set(client_count_key, json!(self.clients.len())) {
            log::error!("Error updating client count: {e}");
        }
    }

    pub fn disconnected(&mut self, client_id: Uuid, remote_addr: SocketAddr) {
        self.clients.remove(&client_id);
        let client_count_key = topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_CLIENTS);
        if let Err(e) = self.set(client_count_key, json!(self.clients.len())) {
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
            if let Err(e) = self.do_unsubscribe(&subscription) {
                log::error!("Inconsistent subscription state: {e}");
            }
        }

        if let Some(grave_goods) = self.grave_goods.remove(&client_id) {
            log::info!("Burying grave goods of client {client_id} ({remote_addr}).");

            for grave_good in grave_goods {
                log::debug!(
                    "Deleting grave good key of client {client_id} ({remote_addr}): {} ",
                    grave_good
                );
                if let Err(e) = self.pdelete(grave_good) {
                    log::error!("Error burying grave goods for client {client_id}: {e}")
                }
            }
        } else {
            log::info!("Client {client_id} ({remote_addr}) has no grave goods.");
        }

        if let Some(last_wills) = self.last_wills.remove(&client_id) {
            log::info!("Publishing last will of client {client_id} ({remote_addr}).");

            for last_will in last_wills {
                log::debug!(
                    "Setting last will of client {client_id} ({remote_addr}): {} = {}",
                    last_will.key,
                    last_will.value
                );
                if let Err(e) = self.set(last_will.key, last_will.value) {
                    log::error!("Error setting last will of client {client_id}: {e}")
                }
            }
        } else {
            log::info!("Client {client_id} ({remote_addr}) has no last will.");
        }
    }
}
