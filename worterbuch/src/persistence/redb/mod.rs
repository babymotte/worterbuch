use crate::{
    Config, Worterbuch,
    persistence::{PersistentStorage, error::PersistenceResult},
    store::Store,
};
use redb::{
    Database, ReadableDatabase, ReadableTable, TableDefinition, TableError, WriteTransaction,
};
use std::{fmt, path::PathBuf};
use tokio::{
    fs, spawn,
    sync::{mpsc, oneshot},
};
use tracing::{debug, error, info, trace, warn};
use worterbuch_common::{
    ClientId, GraveGoods, Key, KeySegment, KeyValuePair, LastWill, ValueEntry, parse_segments,
};

const TABLE_V1: TableDefinition<Key, String> = TableDefinition::new("worterbuch");
const TABLE_V2: TableDefinition<Key, ValueEntry> = TableDefinition::new("worterbuch");
const TABLE_LAST_WILL: TableDefinition<String, LastWill> =
    TableDefinition::new("worterbuch_last_will");
const TABLE_GRAVE_GOODS: TableDefinition<String, GraveGoods> =
    TableDefinition::new("worterbuch_grave_goods");

enum StoreAction {
    Update(Key, ValueEntry),
    UpdateLastWill(ClientId, Option<LastWill>),
    UpdateGraveGoods(ClientId, Option<GraveGoods>),
    Delete(Key),
    Flush(oneshot::Sender<()>),
    Clear,
    Load(oneshot::Sender<Worterbuch>),
}

impl fmt::Debug for StoreAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreAction::Update(key, _) => f.debug_tuple("Update").field(key).finish(),
            StoreAction::UpdateLastWill(client_id, _) => {
                f.debug_tuple("UpdateLastWill").field(client_id).finish()
            }
            StoreAction::UpdateGraveGoods(client_id, _) => {
                f.debug_tuple("UpdateGraveGoods").field(client_id).finish()
            }
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
        let path = PathBuf::from(&config.data_dir).join("worterbuch.re.db");

        info!("Using redb persistence with data dir {}", path.display());

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

    async fn update_grave_goods(
        &self,
        client_id: ClientId,
        grave_goods: Option<GraveGoods>,
    ) -> PersistenceResult<()> {
        self.tx
            .send(StoreAction::UpdateGraveGoods(client_id, grave_goods))
            .await
            .ok();
        Ok(())
    }

    async fn update_last_will(
        &self,
        client_id: ClientId,
        last_will: Option<LastWill>,
    ) -> PersistenceResult<()> {
        self.tx
            .send(StoreAction::UpdateLastWill(client_id, last_will))
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
            StoreAction::UpdateLastWill(client_id, last_will) => {
                update_last_will(&mut db, client_id, last_will)
            }
            StoreAction::UpdateGraveGoods(client_id, grave_goods) => {
                update_grave_goods(&mut db, client_id, grave_goods)
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
    trace!("Updating value {key}={value:?}");
    let write_txn = db.begin_write()?;

    let mut table = write_txn.open_table(TABLE_V2)?;
    table.insert(key, value)?;
    batch_process(rx, next_action, table)?;

    trace!("Updating value done, committing db …");
    write_txn.commit()?;
    trace!("Committing db done.");
    Ok(())
}

fn update_last_will(
    db: &mut Database,
    client_id: ClientId,
    last_will: Option<LastWill>,
) -> PersistenceResult<()> {
    trace!("Updating last will of client {client_id}={last_will:?}");
    let write_txn = db.begin_write()?;

    let mut table = write_txn.open_table(TABLE_LAST_WILL)?;
    let key = client_id.to_string();
    match last_will {
        Some(last_will) => _ = table.insert(key, last_will)?,
        None => _ = table.remove(key)?,
    }
    drop(table);

    trace!("Updating last will done, committing db …");
    write_txn.commit()?;
    trace!("Committing db done.");
    Ok(())
}

fn update_grave_goods(
    db: &mut Database,
    client_id: ClientId,
    grave_goods: Option<GraveGoods>,
) -> PersistenceResult<()> {
    trace!("Updating grave goods of client {client_id}={grave_goods:?}");
    let write_txn = db.begin_write()?;

    let mut table = write_txn.open_table(TABLE_GRAVE_GOODS)?;
    let key = client_id.to_string();
    match grave_goods {
        Some(grave_goods) => _ = table.insert(key, grave_goods)?,
        None => _ = table.remove(key)?,
    }
    drop(table);

    trace!("Updating grave goods done, committing db …");
    write_txn.commit()?;
    trace!("Committing db done.");
    Ok(())
}

fn delete_value(
    db: &mut Database,
    key: Key,
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
) -> PersistenceResult<()> {
    let write_txn = db.begin_write()?;

    let mut table = write_txn.open_table(TABLE_V2)?;
    table.remove(key)?;
    batch_process(rx, next_action, table)?;

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

    restore_entries(db, &mut store)?;

    let write_txn = db.begin_write()?;
    apply_pending_grave_goods(&write_txn, &mut store)?;
    apply_pending_last_wills(&write_txn, &mut store)?;
    write_txn.commit()?;

    db.compact()?;

    store.count_entries();
    info!("Data load complete.");
    tx.send(Worterbuch::with_store(store, config.to_owned()))
        .ok();
    Ok(())
}

fn restore_entries(db: &mut Database, store: &mut Store) -> PersistenceResult<()> {
    let read_txn = db.begin_read()?;
    match read_txn.open_table(TABLE_V2) {
        Ok(table) => {
            for entry in table.iter()? {
                let (k, v) = entry?;
                let key = k.value();
                let value = v.value();
                trace!("Read entry {key}={value:?}");
                let path = parse_segments(&key)?;
                store.insert(&path, value, true)?;
            }
        }
        Err(e) => match e {
            TableError::TableTypeMismatch { table, value, .. } => {
                warn!(
                    "Got unexpected value type '{}' in table '{}'. Starting data migration …",
                    value.name(),
                    table
                );
                drop(read_txn);
                migrate_from_v1_to_v2(db)?;
                restore_entries(db, store)?;
            }
            TableError::TableDoesNotExist(name) => {
                warn!("Table '{name}' does not exist, starting with empty store.");
            }
            e => Err(e)?,
        },
    }

    Ok(())
}

fn migrate_from_v1_to_v2(db: &mut Database) -> PersistenceResult<()> {
    let write_txn = db.begin_write()?;

    let new_name = "worterbuch_v1";
    let renamed_table_def = TableDefinition::new(new_name);

    write_txn.rename_table(TABLE_V1, renamed_table_def)?;
    let table_v1 = write_txn.open_table::<String, String>(renamed_table_def)?;
    let mut table_v2 = write_txn.open_table(TABLE_V2)?;

    for entry in table_v1.iter()? {
        let (k, v) = entry?;
        let key = k.value();
        let value = v.value();
        trace!("Migrating entry {}={}", key, value);
        let value = serde_json::from_str::<ValueEntry>(&value)?;
        table_v2.insert(key, value)?;
    }

    drop(table_v1);
    drop(table_v2);

    write_txn.delete_table(renamed_table_def)?;
    write_txn.commit()?;

    db.compact()?;

    info!("Data migration from v1 to v2 complete.");

    Ok(())
}

fn apply_pending_grave_goods(
    write_txn: &WriteTransaction,
    store: &mut Store,
) -> PersistenceResult<()> {
    let mut table = write_txn.open_table(TABLE_V2)?;
    let gg_table = write_txn.open_table(TABLE_GRAVE_GOODS)?;
    for entry in gg_table.iter()? {
        let (k, v) = entry?;
        let key = k.value();
        let value = v.value();
        trace!("Read grave goods entry {key}={value:?}");
        apply_grave_good(store, value, &mut table)?;
    }
    drop(gg_table);
    write_txn.delete_table(TABLE_GRAVE_GOODS)?;
    Ok(())
}

fn apply_pending_last_wills(
    write_txn: &WriteTransaction,
    store: &mut Store,
) -> PersistenceResult<()> {
    let mut table = write_txn.open_table(TABLE_V2)?;
    let lw_table = write_txn.open_table(TABLE_LAST_WILL)?;
    for entry in lw_table.iter()? {
        let (k, v) = entry?;
        let key = k.value();
        let value = v.value();
        trace!("Read last will entry {key}={value:?}");
        apply_last_will(store, value, &mut table)?;
    }
    drop(lw_table);
    write_txn.delete_table(TABLE_LAST_WILL)?;
    Ok(())
}

fn apply_grave_good(
    store: &mut Store,
    grave_goods: GraveGoods,
    table: &mut redb::Table<'_, Key, ValueEntry>,
) -> PersistenceResult<()> {
    for pattern in grave_goods {
        let path = KeySegment::parse(&pattern);
        let (removed, _) = store.delete_matches(&path)?;
        trace!("Found grave goods for pattern {pattern}: {removed:?}");
        for kvp in removed {
            if table.remove(&kvp.key)?.is_some() {
                trace!("Removed entry {} from database.", kvp.key);
            }
        }
    }
    Ok(())
}

fn apply_last_will(
    store: &mut Store,
    last_will: LastWill,
    table: &mut redb::Table<'_, Key, ValueEntry>,
) -> PersistenceResult<()> {
    for KeyValuePair { key, value } in last_will {
        let path = parse_segments(&key)?;
        store.insert_plain(&path, value.clone(), true)?;
        table.insert(key, ValueEntry::Plain(value))?;
    }
    Ok(())
}

fn clear(db: &mut Database) -> PersistenceResult<()> {
    let write_txn = db.begin_write()?;
    write_txn.delete_table(TABLE_V2)?;
    write_txn.delete_table(TABLE_LAST_WILL)?;
    write_txn.delete_table(TABLE_GRAVE_GOODS)?;
    write_txn.commit()?;
    Ok(())
}

fn batch_process(
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
    mut table: redb::Table<'_, Key, ValueEntry>,
) -> PersistenceResult<()> {
    while let Ok(action) = rx.try_recv() {
        match action {
            StoreAction::Update(key, value) => {
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
