use crate::{
    store::{
        DeletionResult, InsertionResult, Node, Store, ValueEntry,
        error::{StoreError, StoreResult},
    },
    subscribers,
};
use rocksdb::{DB, DBWithThreadMode, IteratorMode, Options, SingleThreaded};
use serde_json::json;
use std::{borrow::Cow, collections::BTreeSet, ops::Deref, path::Path};
use tracing::info;
use worterbuch_common::{
    CasVersion, KeySegment, KeyValuePair, RegularKeySegment, Value, format_path,
};

type RocksDB = DBWithThreadMode<SingleThreaded>;

pub struct RocksDbStore {
    db: RocksDB,
    len: usize,
}

impl RocksDbStore {
    pub fn new(path: impl AsRef<Path>, create_if_missing: bool) -> StoreResult<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(create_if_missing);
        info!("Opening rocksdb at {:?} …", path.as_ref());
        let db = DB::open(&opts, path.as_ref())?;
        info!("Rocksdb opened.");

        let mut store = Self { db, len: 0 };
        store.update_len();

        Ok(store)
    }

    fn update_len(&mut self) {
        self.len = self.db.iterator(IteratorMode::Start).count()
    }

    fn insert(
        &mut self,
        key: &str,
        value: ValueEntry,
        subs: &subscribers::Node,
    ) -> StoreResult<InsertionResult> {
        let current = self
            .db
            .get(key)?
            .and_then(|it| serde_json::from_slice::<ValueEntry>(&it).ok());

        let (value_existed, value_changed, value) = match (&current, value) {
            (None, ValueEntry::Plain(v)) => {
                // no value present, we can insert plain value
                (false, true, ValueEntry::Plain(v))
            }
            (None, ValueEntry::Cas(value, 0)) => {
                // no value present, we can insert cas value if version is 0
                (false, true, ValueEntry::Cas(value, 1))
            }
            (None, ValueEntry::Cas(_, _)) => {
                // no value present, we cannot insert cas value if version != 0
                return Err(StoreError::CasVersionMismatch);
            }
            (Some(ValueEntry::Plain(current)), ValueEntry::Plain(val)) => {
                // plain value present, we can insert plain value
                (true, current != &val, ValueEntry::Plain(val))
            }
            (Some(ValueEntry::Plain(current)), ValueEntry::Cas(val, 0)) => {
                // plain value present, we can insert cas value if version is 0
                (true, current != &val, ValueEntry::Cas(val, 1))
            }
            (Some(ValueEntry::Plain(_)), ValueEntry::Cas(_, _)) => {
                // plain value present, we cannot insert cas value if version != 0
                return Err(StoreError::CasVersionMismatch);
            }
            (Some(ValueEntry::Cas(_, _)), ValueEntry::Plain(_)) => {
                // cas value present, we cannot insert plain value
                return Err(StoreError::Cas);
            }
            (Some(ValueEntry::Cas(current, v_curr)), ValueEntry::Cas(val, v)) if v_curr == &v => {
                // cas value present, we can insert new cas value if the version matches
                (true, current != &val, ValueEntry::Cas(val, v + 1))
            }
            (Some(ValueEntry::Cas(_, _)), ValueEntry::Cas(_, _)) => {
                // cas value present, we cannot insert cas value with mismatched version
                return Err(StoreError::CasVersionMismatch);
            }
        };

        let value = json!(value).to_string();

        self.db.put(key, value)?;

        if !value_existed {
            self.len += 1;
        }

        // TODO collect subscribers
        Ok(InsertionResult {
            value_changed,
            subs: vec![/* TODO */],
            ls_subs: vec![/* TODO */],
        })
    }

    fn matches(key: impl AsRef<str>, pattern: &[KeySegment]) -> bool {
        // We  assume the pattern has already been checked for validity at this point, so we
        // ignore corner cases like a '/#/' in the middle of the pattern, which would be illegal

        let key = key.as_ref();
        let mut key = key.split("/");
        let mut pattern = pattern.iter();

        loop {
            let key_seg = key.next();
            let pat_seg = pattern.next();

            match (key_seg, pat_seg) {
                (Some(k), Some(p)) => {
                    if p == &KeySegment::MultiWildcard {
                        // end of pattern, anything in the key that comes after this is matched,
                        // no way for the match to fail from here
                        return true;
                    }
                    if p != &KeySegment::Wildcard && k != p.deref() {
                        // p is not a wildcard and does not macht k, so there is no match
                        return false;
                    }
                }
                (Some(_), None) | (None, Some(_)) => {
                    // key and pattern have different lengths and the rest of the key is not covered
                    // by a multi wildcard, no match
                    return false;
                }
                (None, None) => {
                    // if no mismatch occurred to this point, the key matches the pattern
                    return true;
                }
            }
        }
    }
}

impl Store for RocksDbStore {
    fn len(&self) -> usize {
        self.len
    }

    fn insert_plain(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
        subs: &subscribers::Node,
    ) -> StoreResult<InsertionResult> {
        let key = path.join("/");
        self.insert(&key, ValueEntry::Plain(value), subs)
    }

    fn insert_cas(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
        version: u64,
        subs: &subscribers::Node,
    ) -> StoreResult<InsertionResult> {
        let key = path.join("/");
        self.insert(&key, ValueEntry::Cas(value, version), subs)
    }

    fn get(&self, path: &[RegularKeySegment]) -> Option<Cow<'_, Value>> {
        let key = path.join("/");
        let value = self.db.get(key).ok()??;

        let value = serde_json::from_slice::<ValueEntry>(&value).ok()?;
        Some(Cow::Owned(value.into()))
    }

    fn cget(&self, path: &[RegularKeySegment]) -> Option<(Cow<'_, Value>, CasVersion)> {
        let key = path.join("/");
        let value = self.db.get(key).ok()??;

        let value = serde_json::from_slice::<ValueEntry>(&value).ok()?;
        match value {
            ValueEntry::Cas(value, v) => Some((Cow::Owned(value), v)),
            ValueEntry::Plain(value) => Some((Cow::Owned(value), 0)),
        }
    }

    fn pget(&self, path: &[KeySegment]) -> StoreResult<Vec<KeyValuePair>> {
        let wc_idx = path.iter().position(|it| it == &KeySegment::Wildcard);
        let mwc_idx = path.iter().position(|it| it == &KeySegment::MultiWildcard);
        let end = wc_idx
            .unwrap_or(usize::MAX)
            .min(mwc_idx.unwrap_or(usize::MAX))
            .min(path.len());
        let prefix = format_path(&path[..end]);

        let iter = self.db.prefix_iterator(prefix);

        let mut res = vec![];

        for it in iter {
            let (k, v) = it?;
            let key = String::from_utf8_lossy(&k);
            if RocksDbStore::matches(&key, path) {
                let value = serde_json::from_slice::<Value>(&v)?;
                res.push(KeyValuePair {
                    key: key.to_string(),
                    value,
                });
            }
        }

        Ok(res)
    }

    fn delete(&mut self, path: &[RegularKeySegment]) -> StoreResult<DeletionResult> {
        let key = path.join("/");
        let value = self.db.get(&key)?;
        let value = if let Some(value) = value {
            Some(KeyValuePair::of(
                key,
                serde_json::from_slice::<ValueEntry>(&value)?,
            ))
        } else {
            None
        };

        let deleted = if let Some(kvp) = value {
            vec![kvp]
        } else {
            vec![]
        };

        // TODO collect subscribers
        Ok(DeletionResult {
            deleted,
            subs: vec![/* TODO */],
            ls_subs: vec![/* TODO */],
        })
    }

    fn pdelete(&mut self, path: &[KeySegment]) -> StoreResult<DeletionResult> {
        let deleted = self.pget(path)?;
        for k in deleted.iter().map(|it| &it.key) {
            self.db.delete(k)?;
        }

        // TODO collect subscribers
        Ok(DeletionResult {
            deleted,
            subs: vec![/* TODO */],
            ls_subs: vec![/* TODO */],
        })
    }

    fn ls(&self, path: Option<&[impl AsRef<str>]>) -> StoreResult<BTreeSet<RegularKeySegment>> {
        let iter = if let Some(parent) = path {
            self.db.prefix_iterator(format_path(parent))
        } else {
            self.db.iterator(IteratorMode::Start)
        };
        let mut children = BTreeSet::new();
        for it in iter {
            let it = it?;
            let it = &String::from_utf8_lossy(&it.0)
                [path.as_ref().map(|it| it.len() + 1).unwrap_or(0)..];
            if let Some(child) = it.split('/').next() {
                children.insert(child.to_owned());
            }
        }
        Ok(children)
    }

    fn pls(&self, path: &[KeySegment]) -> StoreResult<BTreeSet<RegularKeySegment>> {
        todo!()
    }

    fn export(&mut self) -> Node<ValueEntry> {
        todo!()
    }

    fn import(
        &mut self,
        data: Node<ValueEntry>,
        subs: &subscribers::Node,
    ) -> Vec<(KeyValuePair, InsertionResult)> {
        todo!()
    }

    fn reset(&mut self, data: Node<ValueEntry>) {
        todo!()
    }
}
