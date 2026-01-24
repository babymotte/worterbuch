use crate::{
    Config, Worterbuch,
    persistence::{PersistentStorage, error::PersistenceResult},
    store::Store,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::{fmt, path::PathBuf};
use tokio::{
    fs, spawn,
    sync::{mpsc, oneshot},
};
use tracing::{debug, error, info, trace};
use worterbuch_common::{Key, ValueEntry};

const TABLE: TableDefinition<Key, String> = TableDefinition::new("worterbuch");

enum StoreAction {
    Update(Key, ValueEntry),
    Delete(Key),
    Flush(oneshot::Sender<()>),
    Clear,
    Load(oneshot::Sender<Worterbuch>),
}

impl fmt::Debug for StoreAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreAction::Update(key, _) => f.debug_tuple("Update").field(key).finish(),
            StoreAction::Delete(key) => f.debug_tuple("Delete").field(key).finish(),
            StoreAction::Flush(_) => f.debug_tuple("Flush").finish(),
            StoreAction::Clear => f.debug_tuple("Clear").finish(),
            StoreAction::Load(_) => f.debug_tuple("Load").finish(),
        }
    }
}

pub struct PersistentRedbStore {
    tx: mpsc::Sender<StoreAction>,
}

impl PersistentRedbStore {
    pub async fn new(config: &Config) -> PersistenceResult<Self> {
        info!("Using redb persistence.");
        let path = PathBuf::from(&config.data_dir).join("worterbuch.re.db");

        fs::create_dir_all(&config.data_dir).await?;

        let db = Database::create(path)?;
        let (tx, rx) = mpsc::channel(config.channel_buffer_size);

        spawn(run(db, rx, config.clone()));

        Ok(Self { tx })
    }
}

impl PersistentStorage for PersistentRedbStore {
    async fn update_value(&self, key: &Key, value: &ValueEntry) -> PersistenceResult<()> {
        self.tx
            .send(StoreAction::Update(key.clone(), value.clone()))
            .await
            .ok();
        Ok(())
    }

    async fn delete_value(&self, key: &Key) -> PersistenceResult<()> {
        self.tx.send(StoreAction::Delete(key.clone())).await.ok();
        Ok(())
    }

    async fn flush(&mut self, _: &mut Worterbuch) -> PersistenceResult<()> {
        trace!("Triggering ReDB flush …");
        let (tx, rx) = oneshot::channel();
        self.tx.send(StoreAction::Flush(tx)).await.ok();
        trace!("Flush requested.");
        rx.await.ok();
        trace!("ReDB flushed.");
        Ok(())
    }

    async fn load(&self, _: &Config) -> PersistenceResult<Worterbuch> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(StoreAction::Load(tx)).await.ok();

        Ok(rx.await?)
    }

    async fn clear(&self) -> PersistenceResult<()> {
        self.tx.send(StoreAction::Clear).await.ok();
        Ok(())
    }
}
async fn run(mut db: Database, mut rx: mpsc::Receiver<StoreAction>, config: Config) {
    let mut next_action = None;

    loop {
        let action = if let Some(action) = next_action.take() {
            trace!("Next action already scheduled, running it …");
            action
        } else {
            trace!("No action scheduled yet, waiting to receive one …");
            match rx.recv().await {
                Some(action) => {
                    trace!("Received store action {action:?}");
                    action
                }
                None => {
                    debug!("Store action channel closed.");
                    break;
                }
            }
        };

        trace!("Processing store action {action:?} …");

        if let Err(e) = match action {
            StoreAction::Update(key, value) => {
                update_value(&mut db, key, value, &mut rx, &mut next_action)
            }
            StoreAction::Delete(key) => delete_value(&mut db, key, &mut rx, &mut next_action),
            StoreAction::Flush(tx) => flush(&mut db, tx),
            StoreAction::Clear => clear(&mut db),
            StoreAction::Load(tx) => load(&mut db, config.clone(), tx),
        } {
            error!("Error in ReDB persistence: {e}");
        }

        trace!("Store action processed.");
    }

    info!("ReDB closed.");
}

fn update_value(
    db: &mut Database,
    key: Key,
    value: ValueEntry,
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
) -> PersistenceResult<()> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        let value = serde_json::to_string(&value)?;
        table.insert(key, value)?;
        batch_process(rx, next_action, table)?;
    }
    write_txn.commit()?;
    Ok(())
}

fn delete_value(
    db: &mut Database,
    key: Key,
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
) -> PersistenceResult<()> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.remove(key)?;
        batch_process(rx, next_action, table)?;
    }
    write_txn.commit()?;
    Ok(())
}

fn flush(db: &mut Database, tx: oneshot::Sender<()>) -> PersistenceResult<()> {
    trace!("Compacting ReDB …");
    db.compact()?;
    trace!("ReDB compacted.");
    tx.send(()).ok();
    Ok(())
}

fn load(
    db: &mut Database,
    config: Config,
    tx: oneshot::Sender<Worterbuch>,
) -> PersistenceResult<()> {
    info!("Loading data from ReDB …");
    let mut store = Store::default();
    let read_txn = db.begin_read()?;
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
    info!("Data load complete.");
    tx.send(Worterbuch::with_store(store, config.to_owned()))
        .ok();
    Ok(())
}

fn clear(db: &mut Database) -> PersistenceResult<()> {
    let write_txn = db.begin_write()?;
    write_txn.delete_table(TABLE)?;
    write_txn.commit()?;
    Ok(())
}

fn batch_process(
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
    mut table: redb::Table<'_, Key, String>,
) -> PersistenceResult<()> {
    while let Ok(action) = rx.try_recv() {
        match action {
            StoreAction::Update(key, value) => {
                let value = serde_json::to_string(&value)?;
                table.insert(key, value)?;
            }
            StoreAction::Delete(key) => _ = table.remove(key)?,
            action => {
                *next_action = Some(action);
                break;
            }
        }
    }
    Ok(())
}
