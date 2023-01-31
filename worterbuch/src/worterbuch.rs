use crate::{
    config::Config,
    store::{Store, StoreStats},
    subscribers::{Subscriber, Subscribers, SubscriptionId},
};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_value, Value};
use std::{collections::HashMap, fmt::Display, net::SocketAddr};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
};
use uuid::Uuid;
use worterbuch_common::{
    error::{Context, WorterbuchError, WorterbuchResult},
    GraveGoods, Handshake, KeyValuePairs, LastWill, Path, ProtocolVersion, ProtocolVersions,
    RequestPattern, TransactionId,
};

pub type Subscriptions = HashMap<SubscriptionId, RequestPattern>;

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
    subscribers: Subscribers,
    len: usize,
    last_wills: HashMap<Uuid, LastWill>,
    grave_goods: HashMap<Uuid, GraveGoods>,
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
        let store: Store = from_str(json).context(|| format!("Error parsing JSON"))?;
        let len = store.count_entries();
        Ok(Worterbuch {
            config,
            store,
            len,
            ..Default::default()
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn handshake(
        &mut self,
        client_protocol_versions: &ProtocolVersions,
        last_will: LastWill,
        grave_goods: GraveGoods,
        client_id: Uuid,
    ) -> WorterbuchResult<Handshake> {
        // TODO implement protocol versions properly
        let mut supported_protocol_versions = vec![ProtocolVersion { major: 0, minor: 3 }];

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

        let separator = self.config.separator;
        let wildcard = self.config.wildcard;
        let multi_wildcard = self.config.multi_wildcard;

        let handshake = Handshake {
            protocol_version,
            separator,
            wildcard,
            multi_wildcard,
        };

        Ok(handshake)
    }

    pub fn get<'a>(&self, key: impl AsRef<str>) -> WorterbuchResult<(String, Value)> {
        let path: Vec<&str> = key.as_ref().split(self.config.separator).collect();

        let wildcard = self.config.wildcard.to_string();
        let has_wildcard = path.contains(&wildcard.as_str());
        if has_wildcard {
            return Err(WorterbuchError::IllegalWildcard(key.as_ref().to_owned()));
        }

        let multi_wildcard = self.config.multi_wildcard.to_string();
        let has_multi_wildcard = path.last() == Some(&multi_wildcard.as_str());
        if has_multi_wildcard {
            return Err(WorterbuchError::IllegalMultiWildcard(
                key.as_ref().to_owned(),
            ));
        }

        match self.store.get(&path) {
            Some(value) => {
                let key_value = (key.as_ref().to_owned(), value.to_owned());
                Ok(key_value)
            }
            None => Err(WorterbuchError::NoSuchValue(key.as_ref().to_owned())),
        }
    }

    pub fn set(&mut self, key: impl AsRef<str>, value: Value) -> WorterbuchResult<()> {
        let path: Vec<&str> = key.as_ref().split(self.config.separator).collect();

        let wildcard = self.config.wildcard.to_string();
        let has_wildcard = path.contains(&wildcard.as_str());

        let multi_wildcard = self.config.multi_wildcard.to_string();
        let has_multi_wildcard = path.last() == Some(&multi_wildcard.as_str());

        if has_multi_wildcard || has_wildcard {
            return Err(WorterbuchError::IllegalWildcard(key.as_ref().to_owned()));
        }

        let (inserted, changed) = self.store.insert(&path, value.clone());

        if inserted {
            self.increment_len(1);
        }

        self.notify_subscribers(
            path,
            wildcard,
            multi_wildcard,
            key.as_ref(),
            &value,
            changed,
        );

        Ok(())
    }

    fn increment_len(&mut self, increment: usize) {
        self.len += increment;
    }

    pub fn pget<'a>(&self, pattern: impl AsRef<str>) -> WorterbuchResult<KeyValuePairs> {
        let path: Vec<&str> = pattern.as_ref().split(self.config.separator).collect();

        let wildcard = self.config.wildcard.to_string();
        let has_wildcard = path.contains(&wildcard.as_str());

        let multi_wildcard = self.config.multi_wildcard.to_string();
        let has_multi_wildcard = path.last() == Some(&multi_wildcard.as_str());

        let separator = self.config.separator.to_string();

        if has_multi_wildcard {
            let path = &path[0..path.len() - 1];
            if path.contains(&multi_wildcard.as_str()) {
                return Err(WorterbuchError::MultiWildcardAtIllegalPosition(
                    pattern.as_ref().to_owned(),
                ));
            }

            if has_wildcard {
                Ok(self.store.get_match_children(path, &wildcard, &separator))
            } else {
                Ok(self.store.get_children(path, &separator))
            }
        } else if has_wildcard {
            Ok(self.store.get_matches(&path, &wildcard, &separator))
        } else {
            let value = self.store.get(&path);
            let values = value
                .map(|v| vec![(pattern.as_ref().to_owned(), v.to_owned()).into()])
                .unwrap_or_else(|| vec![]);
            Ok(values)
        }
    }

    pub fn subscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        key: String,
        unique: bool,
    ) -> WorterbuchResult<(UnboundedReceiver<KeyValuePairs>, SubscriptionId)> {
        let path: Vec<&str> = key.split(self.config.separator).collect();
        let matches = match self.get(&key) {
            Ok((key, value)) => Some((key, value)),
            Err(WorterbuchError::NoSuchValue(_)) => None,
            Err(e) => return Err(e),
        };
        let (tx, rx) = unbounded_channel();
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx.clone(),
            unique,
        );
        self.subscribers.add_subscriber(&path, subscriber);
        if let Some((key, value)) = matches {
            tx.send(vec![(key, value).into()])
                .expect("rx is neither closed nor dropped");
        }
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, key);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());
        Ok((rx, subscription))
    }

    pub fn psubscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: String,
        unique: bool,
    ) -> WorterbuchResult<(UnboundedReceiver<KeyValuePairs>, SubscriptionId)> {
        let path: Vec<&str> = pattern.split(self.config.separator).collect();
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
        tx.send(matches).expect("rx is neither closed nor dropped");
        let subscription_id = SubscriptionId::new(client_id, transaction_id);
        self.subscriptions.insert(subscription_id, pattern);
        log::debug!("Total subscriptions: {}", self.subscriptions.len());
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
        let imported_values = self.store.merge(store, self.config.separator);

        for (key, val) in &imported_values {
            let path: Vec<&str> = key.split(self.config.separator).collect();
            self.notify_subscribers(
                path,
                self.config.wildcard.to_string(),
                self.config.multi_wildcard.to_string(),
                key,
                val,
                // TODO only pass true if the value actually changed
                true,
            );
        }

        self.increment_len(imported_values.len());

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
        if let Some(pattern) = self.subscriptions.remove(&subscription) {
            log::debug!("Remaining subscriptions: {}", self.subscriptions.len());
            let pattern: Vec<&str> = pattern.split(self.config.separator).collect();
            if self.subscribers.unsubscribe(&pattern, &subscription) {
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
        path: Vec<&str>,
        wildcard: String,
        multi_wildcard: String,
        key: impl AsRef<str>,
        value: &Value,
        value_changed: bool,
    ) {
        let subscribers = self
            .subscribers
            .get_subscribers(&path, &wildcard, &multi_wildcard);

        let filtered_subscribers: Vec<Subscriber> = subscribers
            .into_iter()
            .filter(|s| value_changed || !s.is_unique())
            .collect();

        log::trace!(
            "Calling {} subscribers: {} = {}",
            filtered_subscribers.len(),
            key.as_ref(),
            value
        );
        for subscriber in filtered_subscribers {
            if let Err(e) =
                subscriber.send(vec![(key.as_ref().to_owned(), value.to_owned()).into()])
            {
                log::trace!("Error calling subscriber: {e}");
                self.subscribers.remove_subscriber(subscriber)
            }
        }
    }

    pub fn disconnected(&mut self, client_id: Uuid, remote_addr: SocketAddr) {
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

        if let Some(last_wills) = self.last_wills.remove(&client_id) {
            log::info!("Triggering last will of client {client_id} ({remote_addr}).");

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

        // TODO process grave goods
    }
}
