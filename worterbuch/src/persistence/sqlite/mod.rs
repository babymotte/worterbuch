mod trie;

use crate::{
    Config, Worterbuch,
    persistence::{PersistentStorage, error::PersistenceResult, sqlite::trie::SqliTrie},
    store::Store,
};
use std::{fmt, path::PathBuf};
use tokio::{
    fs, spawn,
    sync::{mpsc, oneshot},
};
use tracing::{debug, error, info, trace};
use worterbuch_common::{
    ClientId, GraveGoods, Key, KeySegment, KeyValuePair, LastWill, ValueEntry, parse_segments,
};

enum StoreAction {
    Update(Key, ValueEntry),
    UpdateLastWill(ClientId, Option<LastWill>),
    UpdateGraveGoods(ClientId, Option<GraveGoods>),
    Delete(Key),
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
            StoreAction::Clear => f.debug_tuple("Clear").finish(),
            StoreAction::Load(_) => f.debug_tuple("Load").finish(),
        }
    }
}

pub struct PersistentSQLiteStore {
    tx: mpsc::Sender<StoreAction>,
}

impl PersistentSQLiteStore {
    pub async fn new(config: &Config) -> PersistenceResult<Self> {
        let path = PathBuf::from(&config.data_dir).join("worterbuch.sqlite.db");

        info!("Using sqlite persistence with data dir {}", path.display());

        fs::create_dir_all(&config.data_dir).await?;

        let db = SqliTrie::open(path)?;

        let (tx, rx) = mpsc::channel(config.channel_buffer_size);

        spawn(run(db, rx, config.clone()));

        Ok(Self { tx })
    }
}

impl PersistentStorage for PersistentSQLiteStore {
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
        // does nothing
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

async fn run(mut db: SqliTrie, mut rx: mpsc::Receiver<StoreAction>, config: Config) {
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
            StoreAction::Clear => clear(&mut db),
            StoreAction::Load(tx) => load(&mut db, config.clone(), tx),
        } {
            error!("Error in SQLite persistence: {e}");
        }

        trace!("Store action processed.");
    }

    info!("SQLite closed.");
}

fn update_value(
    db: &mut SqliTrie,
    key: Key,
    value: ValueEntry,
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
) -> PersistenceResult<()> {
    trace!("Updating value {key}={value:?}");

    db.with_transaction(|db| {
        db.insert(&key, value)?;
        batch_process(rx, next_action, db)?;
        Ok(())
    })?;

    trace!("Updating values done.");
    Ok(())
}

fn batch_process(
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
    db: &mut SqliTrie,
) -> PersistenceResult<()> {
    while let Ok(action) = rx.try_recv() {
        match action {
            StoreAction::Update(key, value) => {
                db.insert(&key, value)?;
            }
            StoreAction::Delete(key) => _ = db.delete(&key)?,
            action => {
                *next_action = Some(action);
                break;
            }
        }
    }
    Ok(())
}

fn update_last_will(
    db: &mut SqliTrie,
    client_id: ClientId,
    last_will: Option<LastWill>,
) -> PersistenceResult<()> {
    trace!("Updating last will of client {client_id}={last_will:?}");

    db.insert_last_will(client_id, last_will)?;

    Ok(())
}

fn update_grave_goods(
    db: &mut SqliTrie,
    client_id: ClientId,
    grave_goods: Option<GraveGoods>,
) -> PersistenceResult<()> {
    trace!("Updating grave goods of client {client_id}={grave_goods:?}");

    db.insert_grave_goods(client_id, grave_goods)?;

    Ok(())
}

fn delete_value(
    db: &mut SqliTrie,
    key: Key,
    rx: &mut mpsc::Receiver<StoreAction>,
    next_action: &mut Option<StoreAction>,
) -> PersistenceResult<()> {
    db.delete(&key)?;
    batch_process(rx, next_action, db)?;

    Ok(())
}

fn load(
    db: &mut SqliTrie,
    config: Config,
    tx: oneshot::Sender<Worterbuch>,
) -> PersistenceResult<()> {
    info!("Loading data from SQLite …");

    let mut store = Store::default();

    restore_entries(db, &mut store)?;

    apply_pending_grave_goods(db, &mut store)?;
    apply_pending_last_wills(db, &mut store)?;

    store.count_entries();
    info!("Data load complete.");
    tx.send(Worterbuch::with_store(store, config.to_owned()))
        .ok();
    Ok(())
}

fn restore_entries(db: &mut SqliTrie, store: &mut Store) -> PersistenceResult<()> {
    for (key, value) in db.load()? {
        trace!("Read entry {key}={value:?}");
        let path = parse_segments(&key)?;
        store.insert(&path, value, true)?;
    }

    Ok(())
}

fn apply_pending_grave_goods(db: &mut SqliTrie, store: &mut Store) -> PersistenceResult<()> {
    let grave_goods = db.drain_grave_goods()?;
    for (client_id, grave_goods) in grave_goods {
        trace!("Found pending grave goods for client {client_id}: {grave_goods:?}");
        apply_grave_good(store, grave_goods)?;
    }
    Ok(())
}

fn apply_pending_last_wills(db: &mut SqliTrie, store: &mut Store) -> PersistenceResult<()> {
    let last_wills = db.drain_last_wills()?;
    for (client_id, last_will) in last_wills {
        trace!("Found pending last will for client {client_id}: {last_will:?}");
        apply_last_will(store, last_will)?;
    }
    Ok(())
}

fn clear(db: &mut SqliTrie) -> PersistenceResult<()> {
    db.clear()?;
    Ok(())
}

fn apply_grave_good(store: &mut Store, grave_goods: GraveGoods) -> PersistenceResult<()> {
    for pattern in grave_goods {
        let path = KeySegment::parse(&pattern);
        let (removed, _) = store.delete_matches(&path)?;
        trace!("Found grave goods for pattern {pattern}: {removed:?}");
    }
    Ok(())
}

fn apply_last_will(store: &mut Store, last_will: LastWill) -> PersistenceResult<()> {
    for KeyValuePair { key, value } in last_will {
        let path = parse_segments(&key)?;
        store.insert_plain(&path, value.clone(), true)?;
    }
    Ok(())
}
