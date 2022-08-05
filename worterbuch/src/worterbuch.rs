use crate::{
    config::Config,
    store::{Store, StoreStats},
    subscribers::{Subscriber, Subscribers, SubscriptionId},
};
use libworterbuch::{
    codec::{KeyValuePair, KeyValuePairs, Path, TransactionId},
    error::{Context, WorterbuchError, WorterbuchResult},
};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_value, Value};
use std::fmt::Display;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::{unbounded_channel, UnboundedReceiver},
};
use uuid::Uuid;

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
    subscribers: Subscribers,
    len: usize,
}

impl Worterbuch {
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

    pub fn get<'a>(&self, key: impl AsRef<str>) -> WorterbuchResult<(String, String)> {
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

    pub fn set(&mut self, key: impl AsRef<str>, value: String) -> WorterbuchResult<()> {
        let path: Vec<&str> = key.as_ref().split(self.config.separator).collect();

        let wildcard = self.config.wildcard.to_string();
        let has_wildcard = path.contains(&wildcard.as_str());

        let multi_wildcard = self.config.multi_wildcard.to_string();
        let has_multi_wildcard = path.last() == Some(&multi_wildcard.as_str());

        if has_multi_wildcard || has_wildcard {
            return Err(WorterbuchError::IllegalWildcard(key.as_ref().to_owned()));
        }

        if self.store.insert(&path, value.clone()) {
            self.increment_len(1);
        }

        self.notify_subscribers(path, wildcard, multi_wildcard, key.as_ref(), &value);

        Ok(())
    }

    fn increment_len(&mut self, increment: usize) {
        self.len += increment;
    }

    pub fn pget<'a>(&self, pattern: impl AsRef<str>) -> WorterbuchResult<Vec<KeyValuePair>> {
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
        );
        self.subscribers.add_subscriber(&path, subscriber);
        if let Some((key, value)) = matches {
            tx.send(vec![(key, value).into()])
                .expect("rx is neither closed nor dropped");
        }
        Ok((rx, subscription))
    }

    pub fn psubscribe(
        &mut self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: String,
    ) -> WorterbuchResult<(UnboundedReceiver<KeyValuePairs>, SubscriptionId)> {
        let path: Vec<&str> = pattern.split(self.config.separator).collect();
        let matches = self.pget(&pattern)?;
        let (tx, rx) = unbounded_channel();
        let subscription = SubscriptionId::new(client_id, transaction_id);
        let subscriber = Subscriber::new(
            subscription.clone(),
            path.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx.clone(),
        );
        self.subscribers.add_subscriber(&path, subscriber);
        tx.send(matches).expect("rx is neither closed nor dropped");
        Ok((rx, subscription))
    }

    pub fn export(&self) -> WorterbuchResult<Value> {
        let value = to_value(&self.store)
            .context(|| format!("Error generating JSON from worterbuch store during export"))?;
        Ok(value)
    }

    pub fn import(&mut self, json: &str) -> WorterbuchResult<Vec<(String, String)>> {
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
        key_pattern: &str,
        subscription: &SubscriptionId,
    ) -> WorterbuchResult<()> {
        let pattern: Vec<&str> = key_pattern.split(self.config.separator).collect();
        if self.subscribers.unsubscribe(&pattern, subscription) {
            Ok(())
        } else {
            Err(WorterbuchError::NotSubscribed)
        }
    }

    #[cfg(not(feature = "docker"))]
    pub fn stats(&self) -> Stats {
        Stats {
            store_stats: self.store.stats(),
        }
    }

    fn notify_subscribers(
        &mut self,
        path: Vec<&str>,
        wildcard: String,
        multi_wildcard: String,
        key: impl AsRef<str>,
        value: &str,
    ) {
        let subscribers = self
            .subscribers
            .get_subscribers(&path, &wildcard, &multi_wildcard);

        log::debug!(
            "Calling {} subscribers: {} = {}",
            subscribers.len(),
            key.as_ref(),
            value
        );
        for subscriber in subscribers {
            if let Err(e) =
                subscriber.send(vec![(key.as_ref().to_owned(), value.to_owned()).into()])
            {
                log::debug!("Error calling subscriber: {e}");
                self.subscribers.remove_subscriber(subscriber)
            }
        }
    }
}
