use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, HashMap};
use worterbuch_common::{
    error::WorterbuchError, KeySegment, KeyValuePair, KeyValuePairs, RegularKeySegment, Value,
};

type NodeValue = Option<Value>;
type Tree = HashMap<RegularKeySegment, Node>;
type CanDelete = bool;

#[derive(Debug)]
pub enum StoreError {
    IllegalMultiWildcard,
}

impl StoreError {
    pub fn for_pattern(self, pattern: String) -> WorterbuchError {
        match self {
            StoreError::IllegalMultiWildcard => WorterbuchError::IllegalMultiWildcard(pattern),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: NodeValue,
    #[serde(skip_serializing_if = "Tree::is_empty", default = "Tree::default")]
    pub t: Tree,
}

impl Node {
    fn merge(
        &mut self,
        other: Node,
        key: Option<&str>,
        insertions: &mut Vec<(String, Value)>,
        path: &[&str],
    ) {
        if let Some(v) = other.v {
            self.v = Some(v.clone());
            let key = concat_key(&path, key);
            log::debug!("Imported {} = {}", key, v);
            insertions.push((key, v));
        }

        let mut path = path.to_owned();
        if let Some(key) = key {
            path.push(key);
        }

        for (key, node) in other.t {
            let own_node = match self.t.entry(key.clone()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(Node::default()),
            };
            own_node.merge(node, Some(&key), insertions, &path);
        }
    }

    fn count_values(&self) -> usize {
        let mut count = if self.v.is_some() { 1 } else { 0 };
        for child in self.t.values() {
            count += child.count_values();
        }
        count
    }

    fn collect_matches<'p>(
        &self,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &'p [KeySegment],
        matches: &mut Vec<KeyValuePair>,
    ) -> StoreResult<()> {
        if remaining_path.is_empty() {
            if let Some(value) = &self.v {
                let key = traversed_path.join("/");
                matches.push((key, value.to_owned()).into());
            }

            return Ok(());
        }

        let next = &remaining_path[0];
        let tail = &remaining_path[1..];

        match next {
            KeySegment::MultiWildcard => {
                if !tail.is_empty() {
                    return Err(StoreError::IllegalMultiWildcard);
                }

                if let Some(value) = &self.v {
                    let key = traversed_path.join("/");
                    matches.push((key, value.to_owned()).into());
                }

                for (key, node) in &self.t {
                    let mut traversed_path = traversed_path.clone();
                    traversed_path.push(key);
                    node.collect_matches(traversed_path, &[KeySegment::MultiWildcard], matches)?;
                }
            }
            KeySegment::Wildcard => {
                for (key, node) in &self.t {
                    let mut traversed_path = traversed_path.clone();
                    traversed_path.push(key);
                    node.collect_matches(traversed_path, tail, matches)?;
                }
            }
            KeySegment::Regular(elem) => {
                traversed_path.push(elem);
                if let Some(child) = self.t.get(elem) {
                    child.collect_matches(traversed_path, tail, matches)?;
                }
            }
        }

        Ok(())
    }

    fn delete(&mut self, relative_path: &[RegularKeySegment]) -> (NodeValue, CanDelete) {
        if relative_path.is_empty() {
            return (self.v.take(), self.t.is_empty());
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        if let Entry::Occupied(mut e) = self.t.entry(head.to_owned()) {
            let next = e.get_mut();
            let (val, can_delete) = next.delete(tail);
            if can_delete {
                e.remove();
            }
            (val, self.v.is_none() && self.t.is_empty())
        } else {
            (None, self.v.is_none() && self.t.is_empty())
        }
    }

    fn delete_matches(
        &mut self,
        traversed_path: Vec<&str>,
        matches: &mut KeyValuePairs,
        relative_path: &[KeySegment],
    ) -> StoreResult<CanDelete> {
        if relative_path.is_empty() {
            if let Some(value) = self.v.take() {
                let key = traversed_path.join("/");
                matches.push((key, value).into());
            }
            return Ok(self.t.is_empty());
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        match &head {
            KeySegment::MultiWildcard => {
                if !tail.is_empty() {
                    return Err(StoreError::IllegalMultiWildcard);
                }
                self.collect_matches(
                    traversed_path.clone(),
                    &[KeySegment::MultiWildcard],
                    matches,
                )?;
                self.t.clear();
            }
            KeySegment::Wildcard => {
                for id in self
                    .t
                    .keys()
                    .map(ToOwned::to_owned)
                    .collect::<Vec<RegularKeySegment>>()
                {
                    let traversed_path = traversed_path.clone();
                    self.delete_child_matches(&id, traversed_path, matches, tail)?;
                }
            }
            KeySegment::Regular(head) => {
                self.delete_child_matches(head, traversed_path, matches, tail)?;
            }
        }

        Ok(self.v.is_none() && self.t.is_empty())
    }

    fn delete_child_matches<'a>(
        &mut self,
        id: &'a RegularKeySegment,
        mut traversed_path: Vec<&'a str>,
        matches: &mut KeyValuePairs,
        relative_path: &[KeySegment],
    ) -> StoreResult<()> {
        traversed_path.push(&id);
        if let Entry::Occupied(mut e) = self.t.entry(id.to_owned()) {
            let child = e.get_mut();
            let can_delete = child.delete_matches(traversed_path, matches, relative_path)?;
            if can_delete {
                e.remove();
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreStats {
    num_entries: usize,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Store {
    data: Node,
    #[serde(skip_serializing, default = "usize::default")]
    len: usize,
}

impl Store {
    pub fn len(&self) -> usize {
        self.len
    }

    /// retrieve a value for a non-wildcard key
    pub fn get(&self, path: &[RegularKeySegment]) -> Option<&Value> {
        let mut current = &self.data;

        for elem in path {
            if let Some(node) = current.t.get(elem) {
                current = node;
            } else {
                return None;
            }
        }

        current.v.as_ref()
    }

    pub fn delete(&mut self, path: &[RegularKeySegment]) -> Option<Value> {
        let removed = self.data.delete(path).0;
        if removed.is_some() {
            self.len -= 1;
        }
        removed
    }

    /// retrieve values for a key containing at least one single-level wildcard and possibly a multi-level wildcard
    pub fn get_matches(&self, path: &[KeySegment]) -> StoreResult<Vec<KeyValuePair>> {
        let mut matches = Vec::new();
        let traversed = vec![];
        self.data.collect_matches(traversed, path, &mut matches)?;
        Ok(matches)
    }

    pub fn delete_matches(&mut self, path: &[KeySegment]) -> StoreResult<Vec<KeyValuePair>> {
        let mut matches = Vec::new();
        let traversed_path = vec![];
        self.data
            .delete_matches(traversed_path, &mut matches, path)?;
        self.len -= matches.len();
        Ok(matches)
    }

    pub fn insert(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
    ) -> StoreResult<(bool, bool)> {
        let mut current = &mut self.data;

        for elem in path {
            current = match current.t.entry(elem.to_owned()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(Node::default()),
            };
        }

        let (inserted, changed) = if let Some(val) = &current.v {
            (false, val != &value)
        } else {
            (true, true)
        };

        current.v = Some(value);

        if inserted {
            self.len += 1;
        }

        Ok((changed, inserted))
    }

    pub fn ls<'s>(&self, mut path: &[&'s str]) -> Option<Vec<RegularKeySegment>> {
        if path.is_empty() {
            panic!("path must not be empty!");
        }
        let mut current = &self.data;

        for elem in path {
            current = match current.t.get(*elem) {
                Some(e) => e,
                None => return None,
            }
        }

        Some(current.t.keys().map(ToOwned::to_owned).collect())
    }

    pub fn ls_root(&self) -> Vec<RegularKeySegment> {
        self.data.t.keys().map(ToOwned::to_owned).collect()
    }

    pub fn merge(&mut self, other: Store) -> Vec<(String, Value)> {
        let mut insertions = Vec::new();
        let path = Vec::new();
        self.data.merge(other.data, None, &mut insertions, &path);
        self.len = self.data.count_values();
        insertions
    }

    pub fn count_entries(&mut self) {
        self.len = self.data.count_values();
    }
}

fn concat_key(path: &[&str], key: Option<&str>) -> String {
    let mut string = String::new();
    for elem in path {
        string.push_str(elem);
        string.push('/');
    }
    if let Some(key) = key {
        string.push_str(key);
    }
    string
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use worterbuch_common::parse_segments;

    fn reg_key_segs(key: &str) -> Vec<RegularKeySegment> {
        parse_segments(key).unwrap()
    }

    fn key_segs(key: &str) -> Vec<KeySegment> {
        KeySegment::parse(key)
    }

    #[test]
    fn test_insert_get() {
        let path = reg_key_segs("test/a/b");

        let mut store = Store::default();
        store.insert(&path, json!("Hello, World!")).unwrap();

        assert_eq!(store.get(&path), Some(&json!("Hello, World!")));
        assert_eq!(store.get(&reg_key_segs("test/a")), None);
        assert_eq!(store.get(&reg_key_segs("test/a/b/c")), None);
    }

    #[test]
    fn test_insert_delete() {
        let path = reg_key_segs("test/a/b");

        let mut store = Store::default();
        store.insert(&path, json!("Hello, World!")).unwrap();

        // assert_eq!(store.len)

        assert_eq!(store.get(&path), Some(&json!("Hello, World!")));
        assert_eq!(store.get(&reg_key_segs("test/a")), None);
        assert_eq!(store.get(&reg_key_segs("test/a/b/c")), None);

        store.delete(&reg_key_segs("test/a/b")).unwrap();
        assert_eq!(store.get(&reg_key_segs("test/a/b")), None);
    }

    #[test]
    fn test_wildcard() {
        let path0 = reg_key_segs("trolo/a");
        let path1 = reg_key_segs("test/a/b");
        let path2 = reg_key_segs("test/a/c");
        let path3 = reg_key_segs("trolo/a/b");
        let path4 = reg_key_segs("trolo/c/b");
        let path5 = reg_key_segs("trolo/c/b/d");

        let mut store = Store::default();
        store.insert(&path0, json!("0")).unwrap();
        store.insert(&path1, json!("1")).unwrap();
        store.insert(&path2, json!("2")).unwrap();
        store.insert(&path3, json!("3")).unwrap();
        store.insert(&path4, json!("4")).unwrap();
        store.insert(&path5, json!("5")).unwrap();

        let res = store.get_matches(&key_segs("test/a/?")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), json!("1")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/c".to_owned(), json!("2")).into())
            .is_some());

        let res = store.get_matches(&key_segs("trolo/?/b")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), json!("3")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), json!("4")).into())
            .is_some());

        let res = store.get_matches(&key_segs("?/a/b")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), json!("1")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), json!("3")).into())
            .is_some());

        let res = store.get_matches(&key_segs("?/?/b")).unwrap();
        assert_eq!(res.len(), 3);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), json!("1")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), json!("3")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), json!("4")).into())
            .is_some());
    }

    #[test]
    fn test_multi_wildcard() {
        let path0 = reg_key_segs("trolo/a");
        let path1 = reg_key_segs("test/a/b");
        let path2 = reg_key_segs("test/a/c");
        let path3 = reg_key_segs("trolo/a/b");
        let path4 = reg_key_segs("trolo/c/b");
        let path5 = reg_key_segs("trolo/c/b/d");

        let mut store = Store::default();
        store.insert(&path0, json!("0")).unwrap();
        store.insert(&path1, json!("1")).unwrap();
        store.insert(&path2, json!("2")).unwrap();
        store.insert(&path3, json!("3")).unwrap();
        store.insert(&path4, json!("4")).unwrap();
        store.insert(&path5, json!("5")).unwrap();

        let res = store.get_matches(&key_segs("test/a/#")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), json!("1")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/c".to_owned(), json!("2")).into())
            .is_some());

        let res = store.get_matches(&key_segs("trolo/#")).unwrap();
        assert_eq!(res.len(), 4);
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a".to_owned(), json!("0")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), json!("3")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), json!("4")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b/d".to_owned(), json!("5")).into())
            .is_some());

        let res = store.get_matches(&key_segs("#")).unwrap();
        assert_eq!(res.len(), 6);
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a".to_owned(), json!("0")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), json!("1")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/c".to_owned(), json!("2")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), json!("3")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), json!("4")).into())
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b/d".to_owned(), json!("5")).into())
            .is_some());
    }
}
