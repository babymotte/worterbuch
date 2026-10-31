/*
 *  Conversion of legacy JSON stores to SQLite for the Worterbuch server.
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

//! One-time conversion of a legacy JSON store into an SQLite store.
//!
//! This only exists to migrate existing deployments and is to be removed
//! together with the JSON persistence code in the next major release.

use crate::{
    Config,
    persistence::{error::PersistenceResult, json, sqlite::trie::SqliTrie},
    store::StoreNode,
};
use std::{
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    time::Instant,
};
use tokio::task::spawn_blocking;
use tracing::{error, info, warn};

/// If there is no SQLite store at `db_path`, try to load a JSON store from the
/// data dir and write it to `db_path`. The JSON store's files are removed if and
/// only if the conversion succeeded.
///
/// This never fails: if there is no JSON store or the conversion fails, no file
/// is created at `db_path`, the JSON store is left untouched and the regular
/// startup will begin with an empty store.
pub(super) async fn convert_json_store_if_needed(config: &Config, db_path: &Path) {
    match tokio::fs::try_exists(db_path).await {
        Ok(true) => return,
        Ok(false) => {}
        Err(e) => {
            warn!(
                "Could not check if SQLite store {} exists, skipping JSON conversion: {e}",
                db_path.display()
            );
            return;
        }
    }

    info!(
        "No SQLite store found at {}, checking for a JSON store to convert …",
        db_path.display()
    );

    let start = Instant::now();
    match convert(config, db_path).await {
        Ok(Some(entries)) => {
            info!(
                "Converted JSON store with {entries} entries to SQLite store {} in {:?}.",
                db_path.display(),
                start.elapsed()
            );
            remove_json_store(config).await;
        }
        Ok(None) => info!("No JSON store found, nothing to convert."),
        Err(e) => error!("Could not convert JSON store to SQLite: {e}"),
    }
}

/// Files of all JSON store versions, relative to the data dir. The timestamp
/// file is not included since it is also used by the SQLite store.
const JSON_STORE_FILES: &[&str] = &[
    // v1
    ".store.json",
    ".store.json~",
    ".store.sha",
    ".store.sha~",
    // v2
    ".store.a.json",
    ".store.b.json",
    ".gglw.a.json",
    ".gglw.b.json",
    // v2 and v3
    ".toggle",
    // v3
    "store.a.json",
    "store.a.json.sha256",
    "store.b.json",
    "store.b.json.sha256",
    "gglw.a.json",
    "gglw.a.json.sha256",
    "gglw.b.json",
    "gglw.b.json.sha256",
    // v3 temp files left behind by an interrupted write
    "store.a.json.tmp",
    "store.a.json.sha256.tmp",
    "store.b.json.tmp",
    "store.b.json.sha256.tmp",
    "gglw.a.json.tmp",
    "gglw.a.json.sha256.tmp",
    "gglw.b.json.tmp",
    "gglw.b.json.sha256.tmp",
];

/// Remove all files of a JSON store that has been converted. Failing to do so
/// is not critical, since a JSON store is never loaded again once an SQLite
/// store exists, so errors are only logged.
async fn remove_json_store(config: &Config) {
    let dir = PathBuf::from(&config.data_dir);
    for file in JSON_STORE_FILES {
        let path = dir.join(file);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => info!("Removed converted JSON store file {}.", path.display()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => warn!(
                "Could not remove converted JSON store file {}: {e}",
                path.display()
            ),
        }
    }
}

async fn convert(config: &Config, db_path: &Path) -> PersistenceResult<Option<usize>> {
    let worterbuch = json::load(config.clone()).await?;
    if worterbuch.len() == 0 {
        return Ok(None);
    }

    info!(
        "Found JSON store with {} entries, converting …",
        worterbuch.len()
    );

    // grave goods and last wills have already been applied by the JSON loader
    let data = worterbuch.into_store().into_data();
    let db_path = db_path.to_owned();
    spawn_blocking(move || write_db(&data, &db_path))
        .await
        .map_err(io::Error::other)?
        .map(Some)
}

/// Write `data` to a temporary database file first and only move it to
/// `db_path` once it is complete, so that a failed or interrupted conversion
/// never leaves a partial store behind.
fn write_db(data: &StoreNode, db_path: &Path) -> PersistenceResult<usize> {
    let tmp_path = with_suffix(db_path, ".converting");
    remove_db_files(&tmp_path)?;

    let res = SqliTrie::open(&tmp_path).and_then(|mut trie| {
        let entries = trie.import(data)?;
        trie.close()?;
        Ok(entries)
    });

    match res {
        Ok(entries) => {
            fs::rename(&tmp_path, db_path)?;
            if let Some(dir) = db_path.parent() {
                // make sure the rename is persisted; not supported on all platforms
                let _ = fs::File::open(dir).and_then(|d| d.sync_all());
            }
            Ok(entries)
        }
        Err(e) => {
            let _ = remove_db_files(&tmp_path);
            Err(e)
        }
    }
}

fn remove_db_files(path: &Path) -> io::Result<()> {
    for path in [
        path.to_owned(),
        with_suffix(path, "-wal"),
        with_suffix(path, "-shm"),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut path: OsString = path.into();
    path.push(suffix);
    path.into()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::persistence::TIMESTAMP_FILE_NAME;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use worterbuch_common::ValueEntry;

    async fn test_config(name: &str) -> Config {
        let data_dir =
            std::env::temp_dir().join(format!("worterbuch-convert-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&data_dir);
        fs::create_dir_all(&data_dir).expect("could not create data dir");

        let mut config = Config::new(None).await.expect("could not create config");
        config.data_dir = data_dir.to_string_lossy().to_string();
        config
    }

    fn data_dir(config: &Config) -> PathBuf {
        PathBuf::from(&config.data_dir)
    }

    fn write_json_store(config: &Config, store: &serde_json::Value) {
        let dir = data_dir(config);
        let json = store.to_string();
        let checksum = hex::encode(Sha256::digest(json.as_bytes()));
        fs::write(dir.join(".toggle"), "").expect("write failed");
        fs::write(dir.join("store.a.json"), &json).expect("write failed");
        fs::write(dir.join("store.a.json.sha256"), checksum).expect("write failed");
    }

    fn json_store_exists(config: &Config) -> bool {
        let dir = data_dir(config);
        [".toggle", "store.a.json", "store.a.json.sha256"]
            .iter()
            .all(|f| dir.join(f).exists())
    }

    fn load_db(db_path: &Path) -> Vec<(String, ValueEntry)> {
        let mut trie = SqliTrie::open(db_path).expect("could not open db");
        let mut entries = Vec::new();
        trie.load(|path, value| {
            entries.push((path.join("/"), value));
            Ok(())
        })
        .expect("load failed");
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    #[tokio::test]
    async fn json_store_is_converted_to_sqlite() {
        let config = test_config("json").await;
        write_json_store(
            &config,
            &json!({
                "data": { "t": {
                    "a": { "v": "inner", "t": {
                        "b": { "v": 1 },
                        "c": { "v": { "Cas": [{ "x": true }, 5] } },
                        "empty": { "t": { "nested_empty": {} } }
                    }},
                    "d": { "v": [1, 2] },
                    "$SYS": { "t": { "x": { "v": 1 } } }
                }}
            }),
        );
        let dir = data_dir(&config);
        fs::write(dir.join("store.b.json.tmp"), "").expect("write failed");
        fs::write(dir.join(TIMESTAMP_FILE_NAME), "").expect("write failed");
        fs::write(dir.join("unrelated.txt"), "").expect("write failed");
        let db_path = dir.join("worterbuch.sqlite.db");

        convert_json_store_if_needed(&config, &db_path).await;

        // JSON store files are removed, other files are kept
        for file in JSON_STORE_FILES {
            assert!(!dir.join(file).exists(), "{file}");
        }
        assert!(dir.join(TIMESTAMP_FILE_NAME).exists());
        assert!(dir.join("unrelated.txt").exists());

        assert_eq!(
            load_db(&db_path),
            vec![
                ("a".to_owned(), ValueEntry::Plain(json!("inner"))),
                ("a/b".to_owned(), ValueEntry::Plain(json!(1))),
                ("a/c".to_owned(), ValueEntry::Cas(json!({ "x": true }), 5)),
                ("d".to_owned(), ValueEntry::Plain(json!([1, 2]))),
            ]
        );

        // no nodes without values are persisted
        let conn = rusqlite::Connection::open(&db_path).expect("could not open db");
        let nodes: i64 = conn
            .query_row("SELECT count(*) FROM worterbuch WHERE id != 0", [], |r| {
                r.get(0)
            })
            .expect("query failed");
        assert_eq!(nodes, 4);

        // no temporary files are left behind
        for suffix in [".converting", ".converting-wal", ".converting-shm"] {
            assert!(!with_suffix(&db_path, suffix).exists(), "{suffix}");
        }

        let _ = fs::remove_dir_all(data_dir(&config));
    }

    #[tokio::test]
    async fn existing_sqlite_store_is_not_overwritten() {
        let config = test_config("existing").await;
        write_json_store(&config, &json!({ "data": { "t": { "a": { "v": 1 } } } }));
        let db_path = data_dir(&config).join("worterbuch.sqlite.db");
        SqliTrie::open(&db_path)
            .and_then(|mut trie| {
                trie.with_transaction(|t| t.insert("b", ValueEntry::Plain(json!(2))))?;
                trie.close()
            })
            .expect("could not create db");

        convert_json_store_if_needed(&config, &db_path).await;

        assert_eq!(
            load_db(&db_path),
            vec![("b".to_owned(), ValueEntry::Plain(json!(2)))]
        );
        assert!(json_store_exists(&config));

        let _ = fs::remove_dir_all(data_dir(&config));
    }

    #[tokio::test]
    async fn no_sqlite_store_is_created_without_json_store() {
        let config = test_config("none").await;
        let db_path = data_dir(&config).join("worterbuch.sqlite.db");

        convert_json_store_if_needed(&config, &db_path).await;

        assert!(!db_path.exists());

        let _ = fs::remove_dir_all(data_dir(&config));
    }

    #[tokio::test]
    async fn no_sqlite_store_is_created_from_corrupted_json_store() {
        let config = test_config("corrupted").await;
        write_json_store(&config, &json!({ "data": { "t": { "a": { "v": 1 } } } }));
        fs::write(data_dir(&config).join("store.a.json.sha256"), "garbage").expect("write failed");
        let db_path = data_dir(&config).join("worterbuch.sqlite.db");

        convert_json_store_if_needed(&config, &db_path).await;

        assert!(!db_path.exists());
        assert!(json_store_exists(&config));

        let _ = fs::remove_dir_all(data_dir(&config));
    }
}
