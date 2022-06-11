use std::fmt::Display;

use crate::{
    store::{Store, StoreStats},
    subscribers::{Subscriber, Subscribers},
};
use anyhow::{Context, Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_value, Value};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use uuid::Uuid;
use worterbuch::config::Config;

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
}

impl Worterbuch {
    pub fn with_config(config: Config) -> Worterbuch {
        Worterbuch {
            config,
            ..Default::default()
        }
    }

    pub fn set(&mut self, key: impl AsRef<str>, value: String) -> Result<()> {
        let path: Vec<&str> = key.as_ref().split(self.config.separator).collect();

        let wildcard = self.config.wildcard.to_string();
        let has_wildcard = path.contains(&wildcard.as_str());

        let multi_wildcard = self.config.multi_wildcard.to_string();
        let has_multi_wildcard = path.last() == Some(&multi_wildcard.as_str());

        if has_multi_wildcard || has_wildcard {
            return Err(Error::msg(
                "cannot set value of a key containing a wildcard",
            ));
        }

        self.store.insert(&path, value.clone());

        self.notify_subscribers(path, wildcard, multi_wildcard, &key, value.as_ref());

        Ok(())
    }

    // pub fn get(&self, key: impl AsRef<str>) -> Result<Option<&str>> {
    //     let path: Vec<&str> = key.as_ref().split(self.config.separator).collect();

    //     let wildcard = self.config.wildcard.to_string();
    //     let has_wildcard = path.contains(&wildcard.as_str());

    //     let multi_wildcard = self.config.multi_wildcard.to_string();
    //     let has_multi_wildcard = path.last() == Some(&multi_wildcard.as_str());

    //     if has_multi_wildcard || has_wildcard {
    //         return Err(Error::msg(
    //             "cannot get a single value from a pattern containing wildcards",
    //         ));
    //     }

    //     Ok(self.store.get(&path))
    // }

    pub fn get_all<'a>(&self, key: impl AsRef<str>) -> Result<Vec<(String, String)>> {
        let path: Vec<&str> = key.as_ref().split(self.config.separator).collect();

        let wildcard = self.config.wildcard.to_string();
        let has_wildcard = path.contains(&wildcard.as_str());

        let multi_wildcard = self.config.multi_wildcard.to_string();
        let has_multi_wildcard = path.last() == Some(&multi_wildcard.as_str());

        let separator = self.config.separator.to_string();

        if has_multi_wildcard {
            let path = &path[0..path.len() - 1];
            if path.contains(&multi_wildcard.as_str()) {
                return Err(Error::msg(
                    "multi level wildcards are only allowed at the end of a path",
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
                .map(|v| vec![(key.as_ref().to_owned(), v.to_owned())])
                .unwrap_or_else(|| vec![]);
            Ok(values)
        }
    }

    pub fn subscribe(
        &mut self,
        key_pattern: String,
    ) -> Result<(UnboundedReceiver<(String, String)>, Uuid)> {
        let path: Vec<&str> = key_pattern.split(self.config.separator).collect();
        let matches = self.get_all(&key_pattern)?;
        let (tx, rx) = unbounded_channel();
        let subscriber = Subscriber::new(
            path.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx.clone(),
        );
        let subscription = subscriber.id().clone();
        self.subscribers.add_subscriber(&path, subscriber);
        for item in matches {
            tx.send(item)?;
        }
        Ok((rx, subscription))
    }

    pub fn export(&self) -> Result<Value> {
        let value = to_value(&self.store)?;
        Ok(value)
    }

    pub fn import(&mut self, json: &str) -> Result<Vec<(String, String)>> {
        log::debug!("Parsing store data …");
        let store: Store = from_str(json).context("Parsing JSON failed")?;
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
        Ok(imported_values)
    }

    pub fn unsubscribe(&mut self, key_pattern: &str, subscription: Uuid) {
        let pattern: Vec<&str> = key_pattern.split(self.config.separator).collect();
        self.subscribers.unsubscribe(&pattern, subscription);
    }

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
            if let Err(e) = subscriber.send((key.as_ref().to_owned(), value.to_owned())) {
                log::debug!("Error calling subscriber: {e}");
                self.subscribers.remove_subscriber(subscriber)
            }
        }
    }
}
