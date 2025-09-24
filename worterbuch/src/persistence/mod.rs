pub mod error;
mod json;

use crate::{
    Config, Worterbuch,
    persistence::{error::PersistenceResult, json::PersistentJsonStorage},
    server::common::CloneableWbApi,
};
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_graceful_shutdown::SubsystemHandle;
use worterbuch_common::{Key, Value};

lazy_static! {
    static ref PERSISTENCE_LOCKED: AtomicBool = AtomicBool::new(true);
}

pub fn is_persistence_locked() -> bool {
    PERSISTENCE_LOCKED.load(Ordering::Acquire)
}

pub fn unlock_persistence() {
    PERSISTENCE_LOCKED.store(false, Ordering::Release);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PersistenceMode {
    Json,
    ReDB,
    // RocksDB,
    // Sqlite
}

pub trait PersistentStorage {
    fn update_value(&self, key: &Key, value: &Value) -> PersistenceResult<()>;

    fn delete_value(&self, key: &Key) -> PersistenceResult<()>;

    async fn flush(&self, worterbuch: &mut Worterbuch) -> PersistenceResult<()>;

    async fn load(self, config: &Config) -> PersistenceResult<Worterbuch>;

    fn clear(&self) -> PersistenceResult<()>;
}

#[derive(PartialEq)]
pub enum PersistentStorageImpl {
    Json(PersistentJsonStorage),
    Noop,
}

impl Default for PersistentStorageImpl {
    fn default() -> Self {
        PersistentStorageImpl::Noop
    }
}

impl PersistentStorage for PersistentStorageImpl {
    fn update_value(&self, key: &Key, value: &Value) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.update_value(key, value),
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    fn delete_value(&self, key: &Key) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.delete_value(key),
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    async fn flush(&self, worterbuch: &mut Worterbuch) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.flush(worterbuch).await,
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    async fn load(self, config: &Config) -> PersistenceResult<Worterbuch> {
        let res = match self {
            PersistentStorageImpl::Json(s) => s.load(config).await,
            PersistentStorageImpl::Noop => Ok(Worterbuch::with_config(config.clone())),
        };
        unlock_persistence();
        res
    }

    fn clear(&self) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.clear(),
            PersistentStorageImpl::Noop => Ok(()),
        }
    }
}

pub(crate) async fn restore(
    subsys: &SubsystemHandle,
    config: &Config,
    api: &CloneableWbApi,
) -> PersistenceResult<Worterbuch> {
    let persistent_storage = get_storage_instance(subsys, config, api);
    persistent_storage.load(&config).await
}

fn get_storage_instance(
    subsys: &SubsystemHandle,
    config: &Config,
    api: &CloneableWbApi,
) -> PersistentStorageImpl {
    if !config.use_persistence || config.follower {
        return PersistentStorageImpl::Noop;
    }

    match config.persistence_mode {
        PersistenceMode::Json => PersistentStorageImpl::Json(PersistentJsonStorage::new(
            subsys,
            config.clone(),
            api.clone(),
        )),
        PersistenceMode::ReDB => todo!(),
    }
}
