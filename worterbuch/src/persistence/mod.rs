pub mod error;
mod json;
mod redb;

use crate::{
    Config, Worterbuch,
    persistence::{
        error::PersistenceResult, json::PersistentJsonStorage, redb::PersistentRedbStore,
    },
    server::common::CloneableWbApi,
};
use lazy_static::lazy_static;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use strum::EnumString;
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{info, warn};
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

#[derive(Debug, Clone, PartialEq, Serialize, EnumString)]
// #[serde(rename_all = "camelCase")]
pub enum PersistenceMode {
    Json,
    ReDB,
    // RocksDB,
    // Sqlite
}

pub trait PersistentStorage {
    fn update_value(&self, key: &Key, value: &Value) -> PersistenceResult<()>;

    fn delete_value(&self, key: &Key) -> PersistenceResult<()>;

    async fn flush(&mut self, worterbuch: &mut Worterbuch) -> PersistenceResult<()>;

    async fn load(&self, config: &Config) -> PersistenceResult<Worterbuch>;

    fn clear(&self) -> PersistenceResult<()>;
}

#[derive(Default)]
pub enum PersistentStorageImpl {
    Json(Box<PersistentJsonStorage>),
    ReDB(Box<PersistentRedbStore>),
    #[default]
    Noop,
}

impl PersistentStorageImpl {
    pub fn update_value(&self, key: &Key, value: &Value) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.update_value(key, value),
            PersistentStorageImpl::ReDB(s) => s.update_value(key, value),
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    pub fn delete_value(&self, key: &Key) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.delete_value(key),
            PersistentStorageImpl::ReDB(s) => s.delete_value(key),
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    pub async fn flush(&mut self, worterbuch: &mut Worterbuch) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.flush(worterbuch).await,
            PersistentStorageImpl::ReDB(s) => s.flush(worterbuch).await,
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    pub async fn load(&self, config: &Config) -> Worterbuch {
        let res = match self {
            PersistentStorageImpl::Json(s) => s.load(config).await,
            PersistentStorageImpl::ReDB(s) => s.load(config).await,
            PersistentStorageImpl::Noop => Ok(Worterbuch::with_config(config.clone())),
        };

        match res {
            Ok(it) => it,
            Err(e) => {
                warn!("Could not restore worterbuch from persistence: {e}");
                info!("Starting empty instace.");
                Worterbuch::with_config(config.clone())
            }
        }
    }

    pub fn clear(&self) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.clear(),
            PersistentStorageImpl::ReDB(s) => s.clear(),
            PersistentStorageImpl::Noop => Ok(()),
        }
    }
}

pub(crate) async fn restore(
    subsys: &SubsystemHandle,
    config: &Config,
    api: &CloneableWbApi,
) -> PersistenceResult<Worterbuch> {
    let persistent_storage = get_storage_instance(subsys, config, api).await?;
    let mut wb = persistent_storage.load(config).await;
    wb.set_persistent_storage(persistent_storage);
    unlock_persistence();
    Ok(wb)
}

async fn get_storage_instance(
    subsys: &SubsystemHandle,
    config: &Config,
    api: &CloneableWbApi,
) -> PersistenceResult<PersistentStorageImpl> {
    if !config.use_persistence || config.follower {
        return Ok(PersistentStorageImpl::Noop);
    }

    let storage =
        match config.persistence_mode {
            PersistenceMode::Json => PersistentStorageImpl::Json(Box::new(
                PersistentJsonStorage::new(subsys, config.clone(), api.clone()),
            )),
            PersistenceMode::ReDB => {
                PersistentStorageImpl::ReDB(Box::new(PersistentRedbStore::new(config).await?))
            }
        };

    Ok(storage)
}
