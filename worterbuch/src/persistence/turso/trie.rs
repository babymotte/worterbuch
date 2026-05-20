use crate::persistence::error::PersistenceResult;
use std::{collections::HashMap, io, path::Path};
use turso::Value;
use worterbuch_common::{KeyValuePair, ValueEntry};

const ROOT_ID: i64 = 0;

pub struct TursoTrie {
    conn: turso::Connection,
    node_cache: HashMap<(i64, String), i64>,
}

impl TursoTrie {
    pub async fn open(path: impl AsRef<Path>) -> PersistenceResult<Self> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF-8 path"))?;
        let db = turso::Builder::new_local(path_str).build().await?;
        let conn = db.connect()?;
        let mut trie = Self {
            conn,
            node_cache: HashMap::new(),
        };
        trie.setup().await?;
        Ok(trie)
    }

    async fn setup(&mut self) -> PersistenceResult<()> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS worterbuch (
                     id            INTEGER PRIMARY KEY,
                     parent_id     INTEGER NOT NULL DEFAULT 0,
                     key_segment   TEXT NOT NULL,
                     value         TEXT,
                     UNIQUE(parent_id, key_segment)
                 )",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS grave_goods (
                     client_id TEXT PRIMARY KEY,
                     value     TEXT NOT NULL
                 )",
                (),
            )
            .await?;
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS last_wills (
                     client_id TEXT PRIMARY KEY,
                     value     TEXT NOT NULL
                 )",
                (),
            )
            .await?;
        self.conn
            .execute(
                "INSERT OR IGNORE INTO worterbuch (id, parent_id, key_segment) VALUES (0, 0, '')",
                (),
            )
            .await?;
        Ok(())
    }

    pub async fn insert(&mut self, key: &str, value: ValueEntry) -> PersistenceResult<()> {
        let leaf_id = self.walk_or_create(key).await?;
        let json = serde_json::to_string(&value)?;
        self.conn
            .execute(
                "UPDATE worterbuch SET value = ?1 WHERE id = ?2",
                (json, leaf_id),
            )
            .await?;
        Ok(())
    }

    pub async fn delete(&mut self, key: &str) -> PersistenceResult<bool> {
        let key_segments: Vec<&str> = key.split('/').collect();
        let ids = self.walk_path(key).await?;
        if ids.len() != key_segments.len() {
            return Ok(false);
        }

        let leaf_id = *ids.last().expect("ids are known to be non-empty at this point");
        if !self.has_value(leaf_id).await? {
            return Ok(false);
        }

        self.conn
            .execute(
                "UPDATE worterbuch SET value = NULL WHERE id = ?1",
                (leaf_id,),
            )
            .await?;

        for (i, &node_id) in ids.iter().enumerate().rev() {
            let has_children = self.has_children(node_id).await?;
            let has_value = self.has_value(node_id).await?;

            if !has_children && !has_value {
                let parent_id = if i == 0 { ROOT_ID } else { ids[i - 1] };
                self.node_cache
                    .remove(&(parent_id, key_segments[i].to_string()));
                self.conn
                    .execute("DELETE FROM worterbuch WHERE id = ?1", (node_id,))
                    .await?;
            } else {
                break;
            }
        }

        Ok(true)
    }

    pub async fn load(&mut self) -> PersistenceResult<Vec<(String, ValueEntry)>> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, parent_id, key_segment, value FROM worterbuch WHERE id != 0",
                (),
            )
            .await?;

        let mut nodes: HashMap<i64, (i64, String, Option<String>)> = HashMap::new();
        while let Some(row) = rows.next().await? {
            let id = extract_i64(&row, 0)?;
            let parent_id = extract_i64(&row, 1)?;
            let key_segment = extract_string(&row, 2)?;
            let value = extract_optional_string(&row, 3)?;
            nodes.insert(id, (parent_id, key_segment, value));
        }
        drop(rows);

        let mut result = Vec::new();
        for (&id, (_, _, value_opt)) in &nodes {
            if let Some(value_str) = value_opt {
                let full_key = build_full_key(id, &nodes);
                let value: ValueEntry = serde_json::from_str(value_str)?;
                result.push((full_key, value));
            }
        }

        Ok(result)
    }

    pub async fn begin_transaction(&mut self) -> PersistenceResult<()> {
        self.conn.execute("BEGIN", ()).await?;
        Ok(())
    }

    pub async fn commit_transaction(&mut self) -> PersistenceResult<()> {
        self.conn.execute("COMMIT", ()).await?;
        Ok(())
    }

    pub async fn rollback_transaction(&mut self) -> PersistenceResult<()> {
        let _ = self.conn.execute("ROLLBACK", ()).await;
        Ok(())
    }

    pub async fn clear(&mut self) -> PersistenceResult<()> {
        self.conn.execute("DELETE FROM worterbuch", ()).await?;
        self.conn.execute("DELETE FROM grave_goods", ()).await?;
        self.conn.execute("DELETE FROM last_wills", ()).await?;
        self.node_cache.clear();
        Ok(())
    }

    pub async fn insert_last_will(
        &mut self,
        client_id: uuid::Uuid,
        last_will: Option<Vec<KeyValuePair>>,
    ) -> PersistenceResult<()> {
        let client_id = client_id.to_string();
        if let Some(last_will) = last_will {
            let value = serde_json::to_string(&last_will)?;
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO last_wills (client_id, value) VALUES (?1, ?2)",
                    (client_id, value),
                )
                .await?;
        } else {
            self.conn
                .execute(
                    "DELETE FROM last_wills WHERE client_id = ?1",
                    (client_id,),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn insert_grave_goods(
        &mut self,
        client_id: uuid::Uuid,
        grave_goods: Option<Vec<String>>,
    ) -> PersistenceResult<()> {
        let client_id = client_id.to_string();
        if let Some(grave_goods) = grave_goods {
            let value = serde_json::to_string(&grave_goods)?;
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO grave_goods (client_id, value) VALUES (?1, ?2)",
                    (client_id, value),
                )
                .await?;
        } else {
            self.conn
                .execute(
                    "DELETE FROM grave_goods WHERE client_id = ?1",
                    (client_id,),
                )
                .await?;
        }
        Ok(())
    }

    pub async fn drain_grave_goods(
        &mut self,
    ) -> PersistenceResult<HashMap<uuid::Uuid, Vec<String>>> {
        let mut rows = self
            .conn
            .query("SELECT client_id, value FROM grave_goods", ())
            .await?;
        let mut pairs: Vec<(String, String)> = Vec::new();
        while let Some(row) = rows.next().await? {
            pairs.push((extract_string(&row, 0)?, extract_string(&row, 1)?));
        }
        drop(rows);

        let mut map = HashMap::new();
        for (client_id_str, value_str) in pairs {
            let client_id = client_id_str
                .parse::<uuid::Uuid>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: Vec<String> = serde_json::from_str(&value_str)?;
            map.insert(client_id, value);
        }

        self.conn.execute("DELETE FROM grave_goods", ()).await?;
        Ok(map)
    }

    pub async fn drain_last_wills(
        &mut self,
    ) -> PersistenceResult<HashMap<uuid::Uuid, Vec<KeyValuePair>>> {
        let mut rows = self
            .conn
            .query("SELECT client_id, value FROM last_wills", ())
            .await?;
        let mut pairs: Vec<(String, String)> = Vec::new();
        while let Some(row) = rows.next().await? {
            pairs.push((extract_string(&row, 0)?, extract_string(&row, 1)?));
        }
        drop(rows);

        let mut map = HashMap::new();
        for (client_id_str, value_str) in pairs {
            let client_id = client_id_str
                .parse::<uuid::Uuid>()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let value: Vec<KeyValuePair> = serde_json::from_str(&value_str)?;
            map.insert(client_id, value);
        }

        self.conn.execute("DELETE FROM last_wills", ()).await?;
        Ok(map)
    }

    async fn has_value(&self, id: i64) -> PersistenceResult<bool> {
        let mut rows = self
            .conn
            .query(
                "SELECT value IS NOT NULL FROM worterbuch WHERE id = ?1",
                (id,),
            )
            .await?;
        match rows.next().await? {
            Some(row) => Ok(matches!(row.get_value(0), Ok(Value::Integer(1)))),
            None => Ok(false),
        }
    }

    async fn has_children(&self, id: i64) -> PersistenceResult<bool> {
        let mut rows = self
            .conn
            .query(
                "SELECT EXISTS(SELECT 1 FROM worterbuch WHERE parent_id = ?1)",
                (id,),
            )
            .await?;
        match rows.next().await? {
            Some(row) => Ok(matches!(row.get_value(0), Ok(Value::Integer(1)))),
            None => Ok(false),
        }
    }

    async fn walk_or_create(&mut self, key: &str) -> PersistenceResult<i64> {
        let mut parent_id = ROOT_ID;
        for key_segment in key.split('/') {
            let cache_key = (parent_id, key_segment.to_string());
            if let Some(&id) = self.node_cache.get(&cache_key) {
                parent_id = id;
                continue;
            }
            let affected = self
                .conn
                .execute(
                    "INSERT OR IGNORE INTO worterbuch (parent_id, key_segment) VALUES (?1, ?2)",
                    (parent_id, key_segment.to_string()),
                )
                .await?;
            let id = if affected > 0 {
                self.conn.last_insert_rowid()
            } else {
                let mut rows = self
                    .conn
                    .query(
                        "SELECT id FROM worterbuch WHERE parent_id = ?1 AND key_segment = ?2",
                        (parent_id, key_segment.to_string()),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => extract_i64(&row, 0)?,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("trie node not found for segment '{key_segment}'"),
                        )
                        .into());
                    }
                }
            };
            self.node_cache.insert(cache_key, id);
            parent_id = id;
        }
        Ok(parent_id)
    }

    async fn walk_path(&mut self, key: &str) -> PersistenceResult<Vec<i64>> {
        let mut parent_id = ROOT_ID;
        let mut ids = Vec::new();
        for key_segment in key.split('/') {
            let cache_key = (parent_id, key_segment.to_string());
            if let Some(&id) = self.node_cache.get(&cache_key) {
                ids.push(id);
                parent_id = id;
                continue;
            }
            let mut rows = self
                .conn
                .query(
                    "SELECT id FROM worterbuch WHERE parent_id = ?1 AND key_segment = ?2",
                    (parent_id, key_segment.to_string()),
                )
                .await?;
            match rows.next().await? {
                Some(row) => {
                    let id = extract_i64(&row, 0)?;
                    self.node_cache.insert(cache_key, id);
                    ids.push(id);
                    parent_id = id;
                }
                None => break,
            }
        }
        Ok(ids)
    }
}

fn extract_i64(row: &turso::Row, idx: usize) -> PersistenceResult<i64> {
    match row.get_value(idx)? {
        Value::Integer(i) => Ok(i),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected integer at column {idx}, got {other:?}"),
        )
        .into()),
    }
}

fn extract_string(row: &turso::Row, idx: usize) -> PersistenceResult<String> {
    match row.get_value(idx)? {
        Value::Text(s) => Ok(s.to_string()),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected text at column {idx}, got {other:?}"),
        )
        .into()),
    }
}

fn extract_optional_string(row: &turso::Row, idx: usize) -> PersistenceResult<Option<String>> {
    match row.get_value(idx)? {
        Value::Text(s) => Ok(Some(s.to_string())),
        Value::Null => Ok(None),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected text or null at column {idx}, got {other:?}"),
        )
        .into()),
    }
}

fn build_full_key(mut id: i64, nodes: &HashMap<i64, (i64, String, Option<String>)>) -> String {
    let mut segments = Vec::new();
    while let Some((parent_id, segment, _)) = nodes.get(&id) {
        segments.push(segment.clone());
        if *parent_id == ROOT_ID {
            break;
        }
        id = *parent_id;
    }
    segments.reverse();
    segments.join("/")
}
