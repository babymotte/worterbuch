mod json;
pub use json::*;
use serde::{Deserialize, Serialize};

pub mod error;

use crate::{
    Worterbuch,
    persistence::error::{PersistenceError, PersistenceResult},
};
use worterbuch_common::{Key, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PersistenceMode {
    Json,
    // ReDB,
    // RocksDB,
    // Sqlite
}

pub trait PersistentStorage {
    fn update_value(&self, key: Key, value: Value) -> PersistenceResult<()>;

    fn dalete_value(&self, key: Key) -> PersistenceResult<()>;

    async fn flush(&self, worterbuch: &mut Worterbuch) -> PersistenceResult<()>;

    async fn load(&self) -> PersistenceResult<Worterbuch>;
}

pub struct NoOpPersistentStorage;

impl PersistentStorage for NoOpPersistentStorage {
    fn update_value(&self, key: Key, value: Value) -> PersistenceResult<()> {
        Ok(())
    }

    fn dalete_value(&self, key: Key) -> PersistenceResult<()> {
        Ok(())
    }

    async fn flush(&self, worterbuch: &mut Worterbuch) -> PersistenceResult<()> {
        Ok(())
    }

    async fn load(&self) -> PersistenceResult<Worterbuch> {
        Err(PersistenceError::PersistenceDisabled)
    }
}
