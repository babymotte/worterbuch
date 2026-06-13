pub mod error;
mod json;
#[cfg(feature = "redb")]
mod redb;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

#[cfg(feature = "redb")]
use crate::persistence::redb::PersistentRedbStore;
#[cfg(feature = "sqlite")]
use crate::persistence::sqlite::PersistentSQLiteStore;
#[cfg(feature = "turso")]
use crate::persistence::turso::PersistentTursoStore;
use crate::{
    Config, PersistenceMode, Worterbuch,
    persistence::{error::PersistenceResult, json::PersistentJsonStorage},
    server::CloneableWbApi,
};
use lazy_static::lazy_static;
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use tosub::SubsystemHandle;
use tracing::{info, trace, warn};
use worterbuch_common::{
    ClientId, GraveGoods, INTERNAL_CLIENT_ID, Key, LastWill, SYSTEM_TOPIC_CLIENTS,
    SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_MODE, SYSTEM_TOPIC_ROOT,
    SYSTEM_TOPIC_ROOT_PREFIX, SYSTEM_TOPIC_STORE, ValueEntry, topic,
};

pub const TIMESTAMP_FILE_NAME: &str = "last-persisted";

lazy_static! {
    static ref PERSISTENCE_LOCKED: AtomicBool = AtomicBool::new(true);
}

pub fn is_persistence_locked() -> bool {
    PERSISTENCE_LOCKED.load(Ordering::Acquire)
}

pub fn unlock_persistence() {
    PERSISTENCE_LOCKED.store(false, Ordering::Release);
}

pub trait PersistentStorage {
    async fn update_value(&self, key: &Key, value: &ValueEntry) -> PersistenceResult<()>;

    async fn delete_value(&self, key: &Key) -> PersistenceResult<()>;

    async fn update_grave_goods(
        &self,
        client_id: ClientId,
        grave_goods: Option<GraveGoods>,
    ) -> PersistenceResult<()>;

    async fn update_last_will(
        &self,
        client_id: ClientId,
        last_will: Option<LastWill>,
    ) -> PersistenceResult<()>;

    async fn flush(&mut self, worterbuch: &mut Worterbuch) -> PersistenceResult<()>;

    async fn load(&self, config: &Config) -> PersistenceResult<Worterbuch>;

    async fn clear(&self) -> PersistenceResult<()>;
}

#[derive(Default)]
pub enum PersistentStorageImpl {
    Json(Box<PersistentJsonStorage>),
    #[cfg(feature = "redb")]
    ReDB(Box<PersistentRedbStore>),
    #[cfg(feature = "sqlite")]
    SQLite(Box<PersistentSQLiteStore>),
    #[cfg(feature = "turso")]
    Turso(Box<PersistentTursoStore>),
    #[default]
    Noop,
}

impl PersistentStorageImpl {
    pub async fn update_value(
        &self,
        key: &Key,
        value: &ValueEntry,
        client_id: Option<ClientId>,
    ) -> PersistenceResult<()> {
        if key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX) {
            if let Some(client_id) = client_id {
                if is_grave_goods_topic(key) {
                    let grave_goods = match value {
                        ValueEntry::Cas(value, _) => serde_json::from_value(value.to_owned())?,
                        ValueEntry::Plain(value) => serde_json::from_value(value.to_owned())?,
                    };
                    trace!("Updating grave goods for client {client_id} to {grave_goods:?}");
                    match self {
                        PersistentStorageImpl::Json(s) => {
                            s.update_grave_goods(client_id, grave_goods).await
                        }
                        #[cfg(feature = "redb")]
                        PersistentStorageImpl::ReDB(s) => {
                            s.update_grave_goods(client_id, grave_goods).await
                        }
                        #[cfg(feature = "sqlite")]
                        PersistentStorageImpl::SQLite(s) => {
                            s.update_grave_goods(client_id, grave_goods).await
                        }
                        #[cfg(feature = "turso")]
                        PersistentStorageImpl::Turso(s) => {
                            s.update_grave_goods(client_id, grave_goods).await
                        }
                        PersistentStorageImpl::Noop => Ok(()),
                    }
                } else if is_last_will_topic(key) {
                    let last_will = match value {
                        ValueEntry::Cas(value, _) => serde_json::from_value(value.to_owned())?,
                        ValueEntry::Plain(value) => serde_json::from_value(value.to_owned())?,
                    };
                    trace!("Updating last will for client {client_id} to {last_will:?}");
                    match self {
                        PersistentStorageImpl::Json(s) => {
                            s.update_last_will(client_id, last_will).await
                        }
                        #[cfg(feature = "redb")]
                        PersistentStorageImpl::ReDB(s) => {
                            s.update_last_will(client_id, last_will).await
                        }
                        #[cfg(feature = "sqlite")]
                        PersistentStorageImpl::SQLite(s) => {
                            s.update_last_will(client_id, last_will).await
                        }
                        #[cfg(feature = "turso")]
                        PersistentStorageImpl::Turso(s) => {
                            s.update_last_will(client_id, last_will).await
                        }
                        PersistentStorageImpl::Noop => Ok(()),
                    }
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            }
        } else {
            match self {
                PersistentStorageImpl::Json(s) => s.update_value(key, value).await,
                #[cfg(feature = "redb")]
                PersistentStorageImpl::ReDB(s) => s.update_value(key, value).await,
                #[cfg(feature = "sqlite")]
                PersistentStorageImpl::SQLite(s) => s.update_value(key, value).await,
                #[cfg(feature = "turso")]
                PersistentStorageImpl::Turso(s) => s.update_value(key, value).await,
                PersistentStorageImpl::Noop => Ok(()),
            }
        }
    }

    pub async fn delete_value(&self, key: &Key) -> PersistenceResult<()> {
        if key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX) {
            return Ok(());
        }

        match self {
            PersistentStorageImpl::Json(s) => s.delete_value(key).await,
            #[cfg(feature = "redb")]
            PersistentStorageImpl::ReDB(s) => s.delete_value(key).await,
            #[cfg(feature = "sqlite")]
            PersistentStorageImpl::SQLite(s) => s.delete_value(key).await,
            #[cfg(feature = "turso")]
            PersistentStorageImpl::Turso(s) => s.delete_value(key).await,
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    pub async fn flush(&mut self, worterbuch: &mut Worterbuch) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.flush(worterbuch).await,
            #[cfg(feature = "redb")]
            PersistentStorageImpl::ReDB(s) => s.flush(worterbuch).await,
            #[cfg(feature = "sqlite")]
            PersistentStorageImpl::SQLite(s) => s.flush(worterbuch).await,
            #[cfg(feature = "turso")]
            PersistentStorageImpl::Turso(s) => s.flush(worterbuch).await,
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    pub async fn load(&self, config: &Config) -> Worterbuch {
        let res = if config.follower {
            Ok(Worterbuch::with_config(config.clone()))
        } else {
            match self {
                PersistentStorageImpl::Json(s) => s.load(config).await,
                #[cfg(feature = "redb")]
                PersistentStorageImpl::ReDB(s) => s.load(config).await,
                #[cfg(feature = "sqlite")]
                PersistentStorageImpl::SQLite(s) => s.load(config).await,
                #[cfg(feature = "turso")]
                PersistentStorageImpl::Turso(s) => s.load(config).await,
                PersistentStorageImpl::Noop => Ok(Worterbuch::with_config(config.clone())),
            }
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

    pub async fn clear(&self) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => s.clear().await,
            #[cfg(feature = "redb")]
            PersistentStorageImpl::ReDB(s) => s.clear().await,
            #[cfg(feature = "sqlite")]
            PersistentStorageImpl::SQLite(s) => s.clear().await,
            #[cfg(feature = "turso")]
            PersistentStorageImpl::Turso(s) => s.clear().await,
            PersistentStorageImpl::Noop => Ok(()),
        }
    }

    pub async fn remove_grave_goods_and_last_will(
        &self,
        client_id: ClientId,
    ) -> PersistenceResult<()> {
        match self {
            PersistentStorageImpl::Json(s) => {
                s.update_grave_goods(client_id, None).await?;
                s.update_last_will(client_id, None).await?;
                Ok(())
            }
            #[cfg(feature = "redb")]
            PersistentStorageImpl::ReDB(s) => {
                s.update_grave_goods(client_id, None).await?;
                s.update_last_will(client_id, None).await?;
                Ok(())
            }
            #[cfg(feature = "sqlite")]
            PersistentStorageImpl::SQLite(s) => {
                s.update_grave_goods(client_id, None).await?;
                s.update_last_will(client_id, None).await?;
                Ok(())
            }
            #[cfg(feature = "turso")]
            PersistentStorageImpl::Turso(s) => {
                s.update_grave_goods(client_id, None).await?;
                s.update_last_will(client_id, None).await?;
                Ok(())
            }
            PersistentStorageImpl::Noop => Ok(()),
        }
    }
}

fn is_grave_goods_topic(key: &str) -> bool {
    let mut split = key.split('/');
    (
        Some(SYSTEM_TOPIC_CLIENTS),
        Some(SYSTEM_TOPIC_GRAVE_GOODS),
        None,
    ) == (split.nth(1), split.nth(1), split.next())
}

fn is_last_will_topic(key: &str) -> bool {
    let mut split = key.split('/');
    (
        Some(SYSTEM_TOPIC_CLIENTS),
        Some(SYSTEM_TOPIC_LAST_WILL),
        None,
    ) == (split.nth(1), split.nth(1), split.next())
}

pub(crate) async fn restore(
    subsys: &SubsystemHandle,
    config: &Config,
    api: &CloneableWbApi,
) -> PersistenceResult<Worterbuch> {
    let persistent_storage = get_storage_instance(subsys, config, api).await?;
    let mut wb = persistent_storage.load(config).await;
    wb.set_persistent_storage(persistent_storage);
    wb.set(
        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_STORE, SYSTEM_TOPIC_MODE),
        json!(config.persistence_mode),
        INTERNAL_CLIENT_ID,
        true,
    )
    .await?;
    unlock_persistence();
    Ok(wb)
}

async fn get_storage_instance(
    subsys: &SubsystemHandle,
    config: &Config,
    api: &CloneableWbApi,
) -> PersistenceResult<PersistentStorageImpl> {
    if !config.use_persistence {
        return Ok(PersistentStorageImpl::Noop);
    }

    let flush_periodically = !config.follower;

    let storage =
        match config.persistence_mode {
            PersistenceMode::Json => PersistentStorageImpl::Json(Box::new(
                PersistentJsonStorage::new(subsys, config.clone(), api.clone(), flush_periodically),
            )),
            #[cfg(feature = "redb")]
            PersistenceMode::ReDB => PersistentStorageImpl::ReDB(Box::new(
                PersistentRedbStore::new(subsys, config).await?,
            )),
            #[cfg(feature = "sqlite")]
            PersistenceMode::SQLite => PersistentStorageImpl::SQLite(Box::new(
                PersistentSQLiteStore::new(subsys, config).await?,
            )),
            #[cfg(feature = "turso")]
            PersistenceMode::Turso => PersistentStorageImpl::Turso(Box::new(
                PersistentTursoStore::new(subsys, config).await?,
            )),
        };

    Ok(storage)
}
