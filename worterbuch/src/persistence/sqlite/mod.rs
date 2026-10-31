/*
 *  SQLite-based persistence implementation for the Worterbuch server.
 *
 *  Copyright (C) 2024 Michael Bachmann
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

mod convert;
mod trie;

use crate::{
    Config, Worterbuch,
    persistence::{
        PersistentStorage, TIMESTAMP_FILE_NAME, error::PersistenceResult, sqlite::trie::SqliTrie,
    },
    store::Store,
};
use hashbrown::HashMap;
use std::{
    fmt, fs, mem,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    thread,
    time::Duration,
};
use tokio::{
    runtime, select,
    sync::{mpsc, oneshot},
    time::MissedTickBehavior,
};
use tosub::Subsystem;
use tracing::{debug, error, info, trace};
use worterbuch_common::{
    ClientId, KeySegment, ValueEntry, parse_segments,
    protocol::v1::{GraveGoods, Key, KeyValuePair, LastWill},
};

/// How often pending value changes are written to the database.
const FLUSH_INTERVAL: Duration = Duration::from_millis(10);

/// Actions that are sent to the SQLite thread. Value changes are not sent as actions but
/// collected in [`PendingWrites`].
enum StoreAction {
    UpdateLastWill(ClientId, Option<LastWill>, oneshot::Sender<()>),
    UpdateGraveGoods(ClientId, Option<GraveGoods>, oneshot::Sender<()>),
    Load(oneshot::Sender<Worterbuch>),
    Flush(oneshot::Sender<()>),
    UpdateTimestamp,
}

impl fmt::Debug for StoreAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreAction::UpdateLastWill(client_id, ..) => {
                f.debug_tuple("UpdateLastWill").field(client_id).finish()
            }
            StoreAction::UpdateGraveGoods(client_id, ..) => {
                f.debug_tuple("UpdateGraveGoods").field(client_id).finish()
            }
            StoreAction::Load(_) => f.debug_tuple("Load").finish(),
            StoreAction::Flush(_) => f.debug_tuple("Flush").finish(),
            StoreAction::UpdateTimestamp => f.debug_tuple("UpdateTimestamp").finish(),
        }
    }
}

/// Value changes that have not been written to the database yet. Changes to the same key are
/// coalesced, so only the latest one is written. This keeps the cost of a write in the core
/// system independent of the database and bounds memory by the number of distinct keys.
#[derive(Default)]
struct PendingWrites {
    /// whether the database needs to be cleared before writing `values`
    clear: bool,
    /// `None` means the key was deleted
    values: HashMap<Key, Option<ValueEntry>>,
}

type SharedPendingWrites = Arc<Mutex<PendingWrites>>;

fn lock(pending: &SharedPendingWrites) -> MutexGuard<'_, PendingWrites> {
    // the lock is never held across operations that could panic
    pending.lock().unwrap_or_else(PoisonError::into_inner)
}

pub struct PersistentSQLiteStore {
    tx: mpsc::Sender<StoreAction>,
    pending: SharedPendingWrites,
}

impl PersistentSQLiteStore {
    pub async fn new(subsys: &Subsystem, config: Config) -> PersistenceResult<Self> {
        let path = PathBuf::from(&config.data_dir).join("worterbuch.sqlite.db");
        let timestamp_file_path = PathBuf::from(&config.data_dir).join(TIMESTAMP_FILE_NAME);

        info!("Using sqlite persistence with data dir {}", path.display());

        tokio::fs::create_dir_all(&config.data_dir).await?;

        let db = if config.role.restore_from_persistence() {
            convert::convert_json_store_if_needed(&config, &path).await;
            SqliTrie::open(path)?
        } else {
            SqliTrie::new(path)?
        };

        let (tx, rx) = mpsc::channel(config.channel_buffer_size);
        let pending = SharedPendingWrites::default();

        let txc = tx.clone();
        subsys.spawn("timestamp-update", move |s| async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                select! {
                    biased;
                    _ = s.shutdown_requested() => break,
                    _ = interval.tick() => {
                        if txc.send(StoreAction::UpdateTimestamp).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        let pc = pending.clone();
        thread::spawn(move || run(db, rx, pc, config, timestamp_file_path));

        Ok(Self { tx, pending })
    }
}

impl PersistentStorage for PersistentSQLiteStore {
    async fn update_value(&self, key: &Key, value: &ValueEntry) -> PersistenceResult<()> {
        lock(&self.pending)
            .values
            .insert(key.clone(), Some(value.clone()));
        Ok(())
    }

    async fn update_grave_goods(
        &self,
        client_id: ClientId,
        grave_goods: Option<GraveGoods>,
    ) -> PersistenceResult<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(StoreAction::UpdateGraveGoods(client_id, grave_goods, tx))
            .await;
        rx.await?;
        Ok(())
    }

    async fn update_last_will(
        &self,
        client_id: ClientId,
        last_will: Option<LastWill>,
    ) -> PersistenceResult<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(StoreAction::UpdateLastWill(client_id, last_will, tx))
            .await;
        rx.await?;
        Ok(())
    }

    async fn delete_value(&self, key: &Key) -> PersistenceResult<()> {
        lock(&self.pending).values.insert(key.clone(), None);
        Ok(())
    }

    async fn flush(&mut self, _: &mut Worterbuch) -> PersistenceResult<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(StoreAction::Flush(tx)).await;
        rx.await?;
        Ok(())
    }

    async fn load(&self, _: &Config) -> PersistenceResult<Worterbuch> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(StoreAction::Load(tx)).await;

        Ok(rx.await?)
    }

    async fn clear(&self) -> PersistenceResult<()> {
        let mut pending = lock(&self.pending);
        // anything written before the clear is obsolete
        pending.values.clear();
        pending.clear = true;
        Ok(())
    }
}

fn run(
    db: SqliTrie,
    rx: mpsc::Receiver<StoreAction>,
    pending: SharedPendingWrites,
    config: Config,
    timestamp_file_path: PathBuf,
) {
    // a minimal runtime, so the thread can wait for actions and the flush interval at once
    match runtime::Builder::new_current_thread().enable_time().build() {
        Ok(rt) => rt.block_on(run_loop(db, rx, pending, config, timestamp_file_path)),
        Err(e) => error!("Could not start SQLite persistence: {e}"),
    }

    info!("SQLite closed.");
}

async fn run_loop(
    mut db: SqliTrie,
    mut rx: mpsc::Receiver<StoreAction>,
    pending: SharedPendingWrites,
    config: Config,
    timestamp_file_path: PathBuf,
) {
    let mut interval = tokio::time::interval(FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut update_timestamp = false;

    loop {
        select! {
            _ = interval.tick() => {
                update_timestamp |= flush_pending_writes(&mut db, &pending);
            }
            action = rx.recv() => {
                // write pending changes first, so actions observe all changes made before them
                update_timestamp |= flush_pending_writes(&mut db, &pending);

                let Some(action) = action else {
                    debug!("Store action channel closed.");
                    break;
                };

                trace!("Processing store action {action:?} …");

                if let Err(e) = match action {
                    StoreAction::UpdateLastWill(client_id, last_will, tx) => {
                        update_timestamp = true;
                        update_last_will(&mut db, client_id, last_will, tx)
                    }
                    StoreAction::UpdateGraveGoods(client_id, grave_goods, tx) => {
                        update_timestamp = true;
                        update_grave_goods(&mut db, client_id, grave_goods, tx)
                    }
                    StoreAction::Load(tx) => load(&mut db, config.clone(), tx),
                    StoreAction::Flush(tx) => {
                        let _ = tx.send(());
                        Ok(())
                    }
                    StoreAction::UpdateTimestamp => {
                        if !update_timestamp {
                            continue;
                        }
                        update_timestamp = false;
                        update_timestamp_file(&timestamp_file_path)
                    }
                } {
                    error!("Error in SQLite persistence: {e}");
                }

                trace!("Store action processed.");
            }
        }
    }
}

/// Writes all pending value changes in a single transaction. Returns whether anything was written.
fn flush_pending_writes(db: &mut SqliTrie, pending: &SharedPendingWrites) -> bool {
    let PendingWrites { clear, values } = mem::take(&mut *lock(pending));

    if !clear && values.is_empty() {
        return false;
    }

    if clear && let Err(e) = db.clear() {
        error!("Error clearing SQLite database: {e}");
    }

    if !values.is_empty() {
        trace!("Writing {} pending value(s) …", values.len());
        if let Err(e) = db.with_transaction(|db| {
            for (key, value) in values {
                match value {
                    Some(value) => db.insert(&key, value)?,
                    None => _ = db.delete(&key)?,
                }
            }
            Ok(())
        }) {
            error!("Error writing values to SQLite: {e}");
        }
    }

    true
}

fn update_timestamp_file(
    timestamp_file_path: &PathBuf,
) -> Result<(), super::error::PersistenceError> {
    debug!(
        "Updating timestamp file {} …",
        timestamp_file_path.display()
    );
    if let Err(e) = fs::File::create(timestamp_file_path) {
        error!("Failed to update timestamp file: {e}");
    }
    Ok(())
}

fn update_last_will(
    db: &mut SqliTrie,
    client_id: ClientId,
    last_will: Option<LastWill>,
    tx: oneshot::Sender<()>,
) -> PersistenceResult<()> {
    trace!("Updating last will of client {client_id}={last_will:?}");

    db.insert_last_will(client_id, last_will, tx)?;
    Ok(())
}

fn update_grave_goods(
    db: &mut SqliTrie,
    client_id: ClientId,
    grave_goods: Option<GraveGoods>,
    tx: oneshot::Sender<()>,
) -> PersistenceResult<()> {
    trace!("Updating grave goods of client {client_id}={grave_goods:?}");

    db.insert_grave_goods(client_id, grave_goods, tx)?;
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
    let _ = tx.send(Worterbuch::with_store(store, config.to_owned()));
    Ok(())
}

fn restore_entries(db: &mut SqliTrie, store: &mut Store) -> PersistenceResult<()> {
    db.load(|path, value| {
        trace!(?path, ?value, "read entry");
        store.insert(path, value, true)?;
        Ok(())
    })
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

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use serde_json::json;

    async fn start(name: &str) -> (PersistentSQLiteStore, Config) {
        let dir = std::env::temp_dir().join(format!(
            "worterbuch-sqlite-writer-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = SqliTrie::open(dir.join("worterbuch.sqlite.db")).unwrap();
        let config = Config::new(None).await.unwrap();
        let (tx, rx) = mpsc::channel(16);
        let pending = SharedPendingWrites::default();
        let pc = pending.clone();
        let cfg = config.clone();
        thread::spawn(move || run(db, rx, pc, cfg, dir.join(TIMESTAMP_FILE_NAME)));
        (PersistentSQLiteStore { tx, pending }, config)
    }

    fn plain(value: serde_json::Value) -> ValueEntry {
        ValueEntry::Plain(value)
    }

    async fn flush(store: &mut PersistentSQLiteStore, config: &Config) {
        let mut wb = Worterbuch::with_config(config.clone());
        store.flush(&mut wb).await.unwrap();
    }

    #[tokio::test]
    async fn writes_are_coalesced_and_persisted() {
        let (mut store, config) = start("coalesce").await;

        let a = "test/a".to_owned();
        let b = "test/b".to_owned();
        let c = "test/c".to_owned();
        store.update_value(&a, &plain(json!(1))).await.unwrap();
        store.update_value(&a, &plain(json!(2))).await.unwrap();
        store.update_value(&b, &plain(json!(1))).await.unwrap();
        store.delete_value(&b).await.unwrap();
        store.update_value(&c, &plain(json!(1))).await.unwrap();
        // a single entry per key is pending
        assert_eq!(lock(&store.pending).values.len(), 3);

        flush(&mut store, &config).await;
        assert!(lock(&store.pending).values.is_empty());

        let wb = store.load(&config).await.unwrap();
        assert_eq!(wb.get(&a).unwrap(), json!(2));
        assert!(wb.get(&b).is_err());
        assert_eq!(wb.get(&c).unwrap(), json!(1));
    }

    #[tokio::test]
    async fn clear_discards_earlier_writes_only() {
        let (mut store, config) = start("clear").await;

        let x = "test/x".to_owned();
        let y = "test/y".to_owned();
        store.update_value(&x, &plain(json!(1))).await.unwrap();
        store.update_value(&y, &plain(json!(1))).await.unwrap();
        flush(&mut store, &config).await;

        store.update_value(&y, &plain(json!(2))).await.unwrap();
        store.clear().await.unwrap();
        // written in the same batch as the clear, after re-creating the tables
        store.update_value(&x, &plain(json!(3))).await.unwrap();
        flush(&mut store, &config).await;

        let wb = store.load(&config).await.unwrap();
        assert_eq!(wb.get(&x).unwrap(), json!(3));
        assert!(wb.get(&y).is_err());
    }
}
