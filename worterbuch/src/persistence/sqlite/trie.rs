/*
 *  Trie data structure for SQLite persistence
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

use crate::{persistence::error::PersistenceResult, store::StoreNode};
use hashbrown::HashMap;
use rusqlite::{Connection, params};
use serde_json::Value;
use std::{io, path::Path};
use tokio::sync::oneshot;
use tracing::trace;
use worterbuch_common::{ValueEntry, protocol::v1::KeyValuePair};

const ROOT_ID: i64 = 0;

pub struct SqliTrie {
    conn: Connection,
    // Caches (parent_id, key_segment) -> child node id to skip re-querying shared prefixes.
    node_cache: NodeCache,
}

/// Maps (parent node id, key segment) to child node id. Children are grouped by parent so a
/// lookup can borrow the key segment instead of allocating an owned key.
#[derive(Default)]
struct NodeCache(HashMap<i64, HashMap<String, i64>>);

impl NodeCache {
    fn get(&self, parent_id: i64, key_segment: &str) -> Option<i64> {
        self.0.get(&parent_id)?.get(key_segment).copied()
    }

    fn insert(&mut self, parent_id: i64, key_segment: String, id: i64) {
        self.0.entry(parent_id).or_default().insert(key_segment, id);
    }

    fn remove(&mut self, parent_id: i64, key_segment: &str) {
        if let Some(children) = self.0.get_mut(&parent_id) {
            children.remove(key_segment);
            if children.is_empty() {
                self.0.remove(&parent_id);
            }
        }
    }

    fn clear(&mut self) {
        self.0.clear();
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[cfg(test)]
    fn key_segments(&self) -> impl Iterator<Item = &str> {
        self.0
            .values()
            .flat_map(|children| children.keys().map(String::as_str))
    }
}

impl SqliTrie {
    pub fn new(path: impl AsRef<Path>) -> PersistenceResult<Self> {
        let conn = Connection::open(path)?;
        let trie = Self {
            conn,
            node_cache: NodeCache::default(),
        };
        Ok(trie)
    }

    pub fn open(path: impl AsRef<Path>) -> PersistenceResult<Self> {
        let conn = Connection::open(path)?;
        let trie = Self {
            conn,
            node_cache: NodeCache::default(),
        };
        trie.setup()?;
        Ok(trie)
    }

    fn setup(&self) -> PersistenceResult<()> {
        // PRAGMAs before table creation; foreign_keys OFF lets the self-referential
        // sentinel row (id=0, parent_id=0) insert without a constraint violation.
        trace!("Setting up SQLite database with WAL journal mode, normal sync, and memory cache");
        trace!(
            "Creating 'worterbuch', 'grave_goods', and 'last_wills' tables if they do not exist"
        );
        trace!("Inserting sentinel root node (id=0, parent_id=0, key_segment='')");
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
                self.node_cache.remove(parent_id, key_segment[i]);
                self.conn
                    .prepare_cached("DELETE FROM worterbuch WHERE id = ?1")?
                    .execute(params![node_id])?;
            } else {
                break;
            }
        }

        Ok(true)
    }

    /// Call `f` with the path and value of every entry stored in the trie and
    /// pre-populate the node cache with all inner nodes.
    ///
    /// Nodes are read in a single table scan ordered by id. Since a parent is
    /// always inserted before its children and rowids only ever increase, a
    /// node's ancestors are normally known by the time the node itself is read,
    /// so its path can be reconstructed right away. Entries for which that does
    /// not hold are deferred until the scan is complete.
    pub fn load<F>(&mut self, mut f: F) -> PersistenceResult<()>
    where
        F: FnMut(&[String], ValueEntry) -> PersistenceResult<()>,
    {
        let mut nodes: HashMap<i64, LoadedNode> = HashMap::new();
        let mut node_cache = NodeCache::default();
        let mut deferred_inner_nodes: Vec<i64> = Vec::new();
        let mut deferred_entries: Vec<(i64, String)> = Vec::new();
        let mut path = Vec::new();

        {
            let mut stmt = self.conn.prepare(
                "SELECT id, parent_id, key_segment, value
                 FROM worterbuch
                 WHERE id != 0
                 ORDER BY id",
            )?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let id: i64 = row.get(0)?;
                let parent_id: i64 = row.get(1)?;
                let key_segment: String = row.get(2)?;
                let value: Option<String> = row.get(3)?;

                if parent_id != ROOT_ID {
                    match nodes.get_mut(&parent_id) {
                        Some(parent) => parent.cache_as_inner_node(parent_id, &mut node_cache),
                        None => deferred_inner_nodes.push(parent_id),
                    }
                }
                nodes.insert(
                    id,
                    LoadedNode {
                        parent_id,
                        key_segment,
                        cached: false,
                    },
                );

                if let Some(value) = value {
                    if build_path(&nodes, id, &mut path) {
                        f(&path, parse_value_entry(&value)?)?;
                    } else {
                        deferred_entries.push((id, value));
                    }
                }
            }
        }

        for id in deferred_inner_nodes {
            if let Some(node) = nodes.get_mut(&id) {
                node.cache_as_inner_node(id, &mut node_cache);
            }
        }

        for (id, value) in deferred_entries {
            if !build_path(&nodes, id, &mut path) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("trie node {id} has a missing ancestor"),
                )
                .into());
            }
            f(&path, parse_value_entry(&value)?)?;
        }

        self.node_cache = node_cache;

        Ok(())
    }

    /// Write all entries of `data` into an empty trie in a single transaction.
    /// Returns the number of entries written.
    pub fn import(&mut self, data: &StoreNode) -> PersistenceResult<usize> {
        self.with_transaction(|trie| trie.import_children(ROOT_ID, data))
    }

    fn import_children(&self, parent_id: i64, node: &StoreNode) -> PersistenceResult<usize> {
        let Some(tree) = &node.tree else {
            return Ok(0);
        };

        let mut count = 0;
        for (key_segment, child) in tree {
            let value = child
                .value
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?;
            self.conn
                .prepare_cached(
                    "INSERT INTO worterbuch (parent_id, key_segment, value) VALUES (?1, ?2, ?3)",
                )?
                .execute(params![parent_id, key_segment, value])?;
            let id = self.conn.last_insert_rowid();

            let written = usize::from(value.is_some()) + self.import_children(id, child)?;
            if written == 0 {
                // don't persist nodes that have no values in their entire sub tree
                self.conn
                    .prepare_cached("DELETE FROM worterbuch WHERE id = ?1")?
                    .execute(params![id])?;
            }
            count += written;
        }

        Ok(count)
    }

    /// Merge the WAL into the main database file and close the connection, so
    /// that afterwards the database consists of a single file that can safely
    /// be moved or renamed.
    pub fn close(self) -> PersistenceResult<()> {
        self.conn.execute_batch("PRAGMA journal_mode = DELETE;")?;
        self.conn.close().map_err(|(_, e)| e)?;
        Ok(())
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
            if let Some(id) = self.node_cache.get(parent_id, key_segment) {
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
            self.node_cache
                .insert(parent_id, key_segment.to_owned(), id);
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
            if let Some(id) = self.node_cache.get(parent_id, key_segment) {
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
                    self.node_cache
                        .insert(parent_id, key_segment.to_owned(), id);
                    ids.push(id);
                    parent_id = id;
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(ids)
    }

    pub(crate) fn clear(&mut self) -> PersistenceResult<()> {
        trace!("Dropping 'worterbuch', 'grave_goods', and 'last_wills' tables");
        // node ids are re-assigned once the tables are re-created
        self.node_cache.clear();
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS worterbuch;
             DROP TABLE IF EXISTS grave_goods;
             DROP TABLE IF EXISTS last_wills;",
        )?;
        self.setup()?;
        Ok(())
    }

    pub(crate) fn insert_last_will(
        &self,
        client_id: uuid::Uuid,
        last_will: Option<Vec<KeyValuePair>>,
        tx: oneshot::Sender<()>,
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

        let _ = tx.send(());
        Ok(())
    }

    pub(crate) fn insert_grave_goods(
        &self,
        client_id: uuid::Uuid,
        grave_goods: Option<Vec<String>>,
        tx: oneshot::Sender<()>,
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

        let _ = tx.send(());
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

/// Equivalent to `serde_json::from_str::<ValueEntry>`, but considerably faster,
/// since it avoids serde's buffering of untagged enum variants.
fn parse_value_entry(json: &str) -> serde_json::Result<ValueEntry> {
    let value: Value = serde_json::from_str(json)?;
    if let Value::Object(map) = &value
        && map.len() == 1
        && let Some(Value::Array(cas)) = map.get("Cas")
        && let [cas_value, Value::Number(version)] = cas.as_slice()
        && let Some(version) = version.as_u64()
    {
        return Ok(ValueEntry::Cas(cas_value.clone(), version));
    }
    Ok(ValueEntry::Plain(value))
}

struct LoadedNode {
    parent_id: i64,
    key_segment: String,
    cached: bool,
}

impl LoadedNode {
    fn cache_as_inner_node(&mut self, id: i64, node_cache: &mut NodeCache) {
        if !self.cached {
            self.cached = true;
            node_cache.insert(self.parent_id, self.key_segment.clone(), id);
        }
    }
}

/// Reconstruct the path of node `id` into `path`. Returns `false` if any of the
/// node's ancestors is not (yet) known.
fn build_path(nodes: &HashMap<i64, LoadedNode>, id: i64, path: &mut Vec<String>) -> bool {
    path.clear();
    let mut current = id;
    while current != ROOT_ID {
        // guard against cycles in a corrupted database
        if path.len() >= nodes.len() {
            return false;
        }
        let Some(node) = nodes.get(&current) else {
            return false;
        };
        path.push(node.key_segment.to_owned());
        current = node.parent_id;
    }
    path.reverse();
    true
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "worterbuch-sqlitrie-{name}-{}.db",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn load_all(trie: &mut SqliTrie) -> Vec<(String, ValueEntry)> {
        let mut entries = Vec::new();
        trie.load(|path, value| {
            entries.push((path.join("/"), value));
            Ok(())
        })
        .expect("load failed");
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    #[test]
    fn load_restores_entries_and_populates_inner_node_cache() {
        let path = temp_db_path("load");

        {
            let mut trie = SqliTrie::open(&path).expect("could not open db");
            trie.with_transaction(|t| {
                t.insert("a/b/c", ValueEntry::Plain(json!(1)))?;
                t.insert("a/b", ValueEntry::Plain(json!("inner value")))?;
                t.insert("a/b/d", ValueEntry::Cas(json!({"x": true}), 3))?;
                t.insert("a/e/f/g", ValueEntry::Plain(json!(null)))?;
                t.insert("h", ValueEntry::Plain(json!([1, 2])))?;
                t.insert("i/j", ValueEntry::Plain(json!(2)))?;
                t.insert("i/j", ValueEntry::Plain(json!(3)))?;
                t.delete("a/e/f/g")?;
                Ok(())
            })
            .expect("transaction failed");
        }

        let mut trie = SqliTrie::open(&path).expect("could not reopen db");
        assert!(trie.node_cache.is_empty());

        let entries = load_all(&mut trie);
        assert_eq!(
            entries,
            vec![
                ("a/b".to_owned(), ValueEntry::Plain(json!("inner value"))),
                ("a/b/c".to_owned(), ValueEntry::Plain(json!(1))),
                ("a/b/d".to_owned(), ValueEntry::Cas(json!({"x": true}), 3)),
                ("h".to_owned(), ValueEntry::Plain(json!([1, 2]))),
                ("i/j".to_owned(), ValueEntry::Plain(json!(3))),
            ]
        );

        // only inner nodes are cached: a, a/b, i
        let mut cached: Vec<&str> = trie.node_cache.key_segments().collect();
        cached.sort();
        assert_eq!(cached, vec!["a", "b", "i"]);

        // the cache must be consistent with the database
        trie.with_transaction(|t| {
            t.insert("a/b/k", ValueEntry::Plain(json!(4)))?;
            t.delete("a/b/c")?;
            Ok(())
        })
        .expect("transaction failed");
        let entries = load_all(&mut trie);
        let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["a/b", "a/b/d", "a/b/k", "h", "i/j"]);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn parse_value_entry_matches_serde() {
        for json in [
            r#"1"#,
            r#""text""#,
            r#"null"#,
            r#"{"value":"x"}"#,
            r#"{"Cas":[{"a":1},7]}"#,
            r#"{"Cas":[1,-7]}"#,
            r#"{"Cas":[1,7.5]}"#,
            r#"{"Cas":[1]}"#,
            r#"{"Cas":[1,2,3]}"#,
            r#"{"Cas":[1,2],"other":true}"#,
            r#"{"Cas":"no array"}"#,
            r#"[1,2]"#,
        ] {
            let expected: ValueEntry = serde_json::from_str(json).expect("serde failed");
            assert_eq!(
                parse_value_entry(json).expect("parse failed"),
                expected,
                "{json}"
            );
        }
    }

    #[test]
    fn load_handles_children_with_lower_ids_than_their_parents() {
        let path = temp_db_path("order");

        {
            let trie = SqliTrie::open(&path).expect("could not open db");
            trie.conn
                .execute_batch(
                    r#"INSERT INTO worterbuch (id, parent_id, key_segment, value) VALUES (5, 9, 'leaf', '1');
                       INSERT INTO worterbuch (id, parent_id, key_segment, value) VALUES (9, 0, 'root', NULL);"#,
                )
                .expect("insert failed");
        }

        let mut trie = SqliTrie::open(&path).expect("could not reopen db");
        let entries = load_all(&mut trie);
        assert_eq!(
            entries,
            vec![("root/leaf".to_owned(), ValueEntry::Plain(json!(1)))]
        );

        let _ = std::fs::remove_file(&path);
    }
}
