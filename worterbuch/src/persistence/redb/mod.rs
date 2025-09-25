use crate::{
    Config, Worterbuch,
    persistence::{PersistentStorage, error::PersistenceResult},
    store::{Store, ValueEntry},
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::PathBuf;
use tokio::fs;
use tracing::info;
use worterbuch_common::Key;

const TABLE: TableDefinition<Key, String> = TableDefinition::new("worterbuch");

pub struct PersistentRedbStore {
    db: Database,
}

impl PersistentRedbStore {
    pub async fn new(config: &Config) -> PersistenceResult<Self> {
        info!("Using redb persistence.");
        let path = PathBuf::from(&config.data_dir).join("redb");
        fs::create_dir_all(&config.data_dir).await?;
        let db = Database::create(path)?;
        Ok(Self { db })
    }
}

impl PersistentStorage for PersistentRedbStore {
    fn update_value(&self, key: &Key, value: &ValueEntry) -> PersistenceResult<()> {
        // TODO move to background task and batch operations
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            let value = serde_json::to_string(value)?;
            table.insert(key, value)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn delete_value(&self, key: &Key) -> PersistenceResult<()> {
        // TODO move to background task and batch operations
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    async fn flush(&mut self, _: &mut Worterbuch) -> PersistenceResult<()> {
        self.db.compact()?;
        Ok(())
    }

    async fn load(&self, config: &Config) -> PersistenceResult<Worterbuch> {
        let mut store = Store::default();
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;
        for entry in table.iter()? {
            let (k, v) = entry?;
            let key = k.value();
            let value = serde_json::from_str::<ValueEntry>(&v.value())?;
            let path: Vec<String> = key.split('/').map(ToOwned::to_owned).collect();
            match value {
                ValueEntry::Cas(value, version) => store.insert_cas(&path, value, version)?,
                ValueEntry::Plain(value) => store.insert_plain(&path, value)?,
            };
        }
        store.count_entries();
        Ok(Worterbuch::with_store(store, config.to_owned()))
    }

    fn clear(&self) -> PersistenceResult<()> {
        // TODO move to background task
        let write_txn = self.db.begin_write()?;
        write_txn.delete_table(TABLE)?;
        write_txn.commit()?;
        Ok(())
    }
}
