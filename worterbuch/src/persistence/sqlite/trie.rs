use crate::persistence::error::PersistenceResult;
use rusqlite::{Connection, params};
use std::{collections::HashMap, io, path::Path};
use worterbuch_common::{KeyValuePair, ValueEntry};

const ROOT_ID: i64 = 0;

pub struct SqliTrie {
    conn: Connection,
    // Caches (parent_id, key_segment) -> child node id to skip re-querying shared prefixes.
    node_cache: HashMap<(i64, String), i64>,
}

impl SqliTrie {
    pub fn open(path: impl AsRef<Path>) -> PersistenceResult<Self> {
        let conn = Connection::open(path)?;
        let mut trie = Self {
            conn,
            node_cache: HashMap::new(),
        };
        trie.setup()?;
        Ok(trie)
    }

    fn setup(&mut self) -> PersistenceResult<()> {
        // PRAGMAs before table creation; foreign_keys OFF lets the self-referential
        // sentinel row (id=0, parent_id=0) insert without a constraint violation.
        self.conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -65536;
             PRAGMA temp_store = MEMORY;
             CREATE TABLE IF NOT EXISTS worterbuch (
                 id            INTEGER PRIMARY KEY,
                 parent_id     INTEGER NOT NULL DEFAULT 0,
                 key_segment   TEXT NOT NULL,
                 value         TEXT,
                 UNIQUE(parent_id, key_segment)
             );
             CREATE TABLE IF NOT EXISTS grave_goods (
                 client_id     TEXT PRIMARY KEY,
                 value         TEXT NOT NULL,
                 UNIQUE(client_id)
             );
             CREATE TABLE IF NOT EXISTS last_wills (
                 client_id     TEXT PRIMARY KEY,
                 value         TEXT NOT NULL,
                 UNIQUE(client_id)
             );
             INSERT OR IGNORE INTO worterbuch (id, parent_id, key_segment) VALUES (0, 0, '');",
        )?;
        Ok(())
    }

    /// Insert or update `key` with `value`. Key segments are separated by `/`.
    pub fn insert(&mut self, key: &str, value: ValueEntry) -> PersistenceResult<()> {
        let leaf_id = self.walk_or_create(key)?;
        let json = serde_json::to_string(&value)?;
        self.conn
            .prepare_cached("UPDATE worterbuch SET value = ?1 WHERE id = ?2")?
            .execute(params![json, leaf_id])?;
        Ok(())
    }

    /// Delete `key` and prune any now-empty ancestor nodes bottom-up.
    /// Returns `true` if the key existed.
    pub fn delete(&mut self, key: &str) -> PersistenceResult<bool> {
        let key_segment: Vec<&str> = key.split('/').collect();
        let ids = self.walk_path(key)?;
        if ids.len() != key_segment.len() {
            return Ok(false);
        }

        let leaf_id = *ids
            .last()
            .expect("ids are known to be non-empty at this point");
        let has_value: bool = self
            .conn
            .prepare_cached("SELECT value IS NOT NULL FROM worterbuch WHERE id = ?1")?
            .query_row(params![leaf_id], |row| row.get(0))?;
        if !has_value {
            return Ok(false);
        }

        self.conn
            .prepare_cached("UPDATE worterbuch SET value = NULL WHERE id = ?1")?
            .execute(params![leaf_id])?;

        for (i, &node_id) in ids.iter().enumerate().rev() {
            let has_children: bool = self
                .conn
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM worterbuch WHERE parent_id = ?1)")?
                .query_row(params![node_id], |row| row.get(0))?;
            let has_value: bool = self
                .conn
                .prepare_cached("SELECT value IS NOT NULL FROM worterbuch WHERE id = ?1")?
                .query_row(params![node_id], |row| row.get(0))?;

            if !has_children && !has_value {
                let parent_id = if i == 0 { ROOT_ID } else { ids[i - 1] };
                self.node_cache
                    .remove(&(parent_id, key_segment[i].to_string()));
                self.conn
                    .prepare_cached("DELETE FROM worterbuch WHERE id = ?1")?
                    .execute(params![node_id])?;
            } else {
                break;
            }
        }

        Ok(true)
    }

    /// Iterate over every `(full_key, value)` pair stored in the trie.
    /// Full keys are reconstructed by the recursive CTE, so no additional
    /// round-trips are needed.
    pub fn load(&self) -> PersistenceResult<Vec<(String, ValueEntry)>> {
        let mut stmt = self.conn.prepare_cached(
            "WITH RECURSIVE full_paths(id, full_key, value) AS (
                     SELECT id, key_segment, value
                     FROM worterbuch
                     WHERE parent_id = 0 AND id != 0
                     UNION ALL
                     SELECT w.id, fp.full_key || '/' || w.key_segment, w.value
                     FROM worterbuch w
                     JOIN full_paths fp ON w.parent_id = fp.id
                 )
                 SELECT full_key, value
                 FROM full_paths
                 WHERE value IS NOT NULL",
        )?;

        let pairs = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<(String, String)>>>()?
            .into_iter()
            .map(|(key, val_str)| serde_json::from_str(&val_str).map(|v| (key, v)))
            .collect::<serde_json::error::Result<Vec<_>>>()?;

        Ok(pairs)
    }

    /// Execute `f` inside a single SQLite transaction. Use this to batch many
    /// inserts/deletes for maximum write throughput.
    pub fn with_transaction<F, T>(&mut self, f: F) -> PersistenceResult<T>
    where
        F: FnOnce(&mut Self) -> PersistenceResult<T>,
    {
        self.conn.execute_batch("BEGIN")?;
        match f(self) {
            Ok(val) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(val)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Walk the path creating missing nodes as needed; returns the leaf node id.
    fn walk_or_create(&mut self, key: &str) -> PersistenceResult<i64> {
        let mut parent_id = ROOT_ID;
        for key_segment in key.split('/') {
            let cache_key = (parent_id, key_segment.to_string());
            if let Some(&id) = self.node_cache.get(&cache_key) {
                parent_id = id;
                continue;
            }
            let rows = self
                .conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO worterbuch (parent_id, key_segment) VALUES (?1, ?2)",
                )?
                .execute(params![parent_id, key_segment])?;
            let id: i64 = if rows > 0 {
                self.conn.last_insert_rowid()
            } else {
                self.conn
                    .prepare_cached(
                        "SELECT id FROM worterbuch WHERE parent_id = ?1 AND key_segment = ?2",
                    )?
                    .query_row(params![parent_id, key_segment], |row| row.get(0))?
            };
            self.node_cache.insert(cache_key, id);
            parent_id = id;
        }
        Ok(parent_id)
    }

    /// Walk the path without creating nodes. Stops at the first missing key_segment
    /// and returns the ids collected so far (len < segments means path incomplete).
    fn walk_path(&mut self, key: &str) -> PersistenceResult<Vec<i64>> {
        let mut parent_id = ROOT_ID;
        let mut ids = Vec::new();
        for key_segment in key.split('/') {
            let cache_key = (parent_id, key_segment.to_string());
            if let Some(&id) = self.node_cache.get(&cache_key) {
                ids.push(id);
                parent_id = id;
                continue;
            }
            match self
                .conn
                .prepare_cached(
                    "SELECT id FROM worterbuch WHERE parent_id = ?1 AND key_segment = ?2",
                )?
                .query_row(params![parent_id, key_segment], |row| row.get::<_, i64>(0))
            {
                Ok(id) => {
                    self.node_cache.insert(cache_key, id);
                    ids.push(id);
                    parent_id = id;
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(ids)
    }

    pub(crate) fn clear(&self) -> PersistenceResult<()> {
        self.conn.execute_batch(
            "DELETE FROM worterbuch;
             DELETE FROM grave_goods;
             DELETE FROM last_wills;",
        )?;
        Ok(())
    }

    pub(crate) fn insert_last_will(
        &self,
        client_id: uuid::Uuid,
        last_will: Option<Vec<worterbuch_common::KeyValuePair>>,
    ) -> PersistenceResult<()> {
        let client_id = client_id.to_string();

        if let Some(last_will) = last_will {
            let value = serde_json::to_string(&last_will)?;
            self.conn
                .prepare_cached(
                    "INSERT OR REPLACE INTO last_wills (client_id, value) VALUES (?1, ?2);",
                )?
                .execute(params![client_id, value])?;
        } else {
            self.conn
                .prepare_cached("DELETE FROM last_wills WHERE client_id = ?1;")?
                .execute(params![client_id])?;
        }

        Ok(())
    }

    pub(crate) fn insert_grave_goods(
        &self,
        client_id: uuid::Uuid,
        grave_goods: Option<Vec<String>>,
    ) -> PersistenceResult<()> {
        let client_id = client_id.to_string();

        if let Some(grave_goods) = grave_goods {
            let value = serde_json::to_string(&grave_goods)?;
            self.conn
                .prepare_cached(
                    "INSERT OR REPLACE INTO grave_goods (client_id, value) VALUES (?1, ?2);",
                )?
                .execute(params![client_id, value])?;
        } else {
            self.conn
                .prepare_cached("DELETE FROM grave_goods WHERE client_id = ?1;")?
                .execute(params![client_id])?;
        }
        Ok(())
    }

    pub(crate) fn drain_grave_goods(&self) -> PersistenceResult<HashMap<uuid::Uuid, Vec<String>>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT client_id, value FROM grave_goods")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut map = HashMap::new();
        for (client_id_str, value_str) in rows {
            let client_id = client_id_str
                .parse::<uuid::Uuid>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: Vec<String> = serde_json::from_str(&value_str)?;
            map.insert(client_id, value);
        }

        self.conn.execute_batch("DELETE FROM grave_goods")?;

        Ok(map)
    }

    pub(crate) fn drain_last_wills(
        &self,
    ) -> PersistenceResult<HashMap<uuid::Uuid, Vec<KeyValuePair>>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT client_id, value FROM last_wills")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut map = HashMap::new();
        for (client_id_str, value_str) in rows {
            let client_id = client_id_str
                .parse::<uuid::Uuid>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: Vec<KeyValuePair> = serde_json::from_str(&value_str)?;
            map.insert(client_id, value);
        }

        self.conn.execute_batch("DELETE FROM last_wills")?;

        Ok(map)
    }
}
