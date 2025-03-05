/*
 *  Worterbuch in-memory store module
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

use serde::{Deserialize, Serialize};
use std::collections::{hash_map::Entry, HashMap, HashSet};
use worterbuch_common::{
    error::{WorterbuchError, WorterbuchResult},
    parse_segments, CasVersion, KeySegment, KeyValuePair, KeyValuePairs, RegularKeySegment, Value,
    SYSTEM_TOPIC_ROOT,
};

use crate::subscribers::{LsSubscriber, Subscriber, SubscriptionId};

type NodeValue = Option<ValueEntry>;
type Tree = HashMap<RegularKeySegment, Node>;
type SubscribersTree = HashMap<RegularKeySegment, SubscribersNode>;
type CanDelete = bool;

pub type AffectedLsSubscribers = (Vec<LsSubscriber>, Vec<RegularKeySegment>);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum ValueEntry {
    Plain(Value),
    Cas(Value, u64),
}

impl AsRef<Value> for ValueEntry {
    fn as_ref(&self) -> &Value {
        match self {
            ValueEntry::Plain(value) => value,
            ValueEntry::Cas(value, _) => value,
        }
    }
}

impl From<ValueEntry> for Value {
    fn from(value: ValueEntry) -> Self {
        match value {
            ValueEntry::Plain(value) => value,
            ValueEntry::Cas(value, _) => value,
        }
    }
}

impl From<Value> for ValueEntry {
    fn from(value: Value) -> Self {
        ValueEntry::Plain(value)
    }
}

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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    #[serde(skip_serializing_if = "Option::is_none")]
    v: NodeValue,
    #[serde(skip_serializing_if = "Tree::is_empty", default = "Tree::default")]
    t: Tree,
}

#[derive(Debug, Clone, Default)]
pub struct SubscribersNode {
    _subscribers: Vec<Subscriber>,
    ls_subscribers: Vec<LsSubscriber>,
    pub tree: SubscribersTree,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreStats {
    num_entries: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Store {
    data: Node,
    #[serde(skip_serializing, default = "usize::default")]
    len: usize,
    #[serde(
        skip_serializing,
        skip_deserializing,
        default = "SubscribersNode::default"
    )]
    subscribers: SubscribersNode,
    #[serde(skip_serializing, default = "bool::default")]
    unsaved_changes: bool,
}

impl Store {
    pub fn has_unsaved_changes(&self) -> bool {
        self.unsaved_changes
    }

    pub fn export(&mut self) -> Node {
        Node {
            v: self.data.v.clone(),
            t: self.slim_copy_top_level_children(),
        }
    }

    pub fn export_for_persistence(&mut self) -> Self {
        let data = self.export();

        self.unsaved_changes = false;

        Self {
            data,
            ..Default::default()
        }
    }

    fn slim_copy_top_level_children(&self) -> Tree {
        let mut children = Tree::new();
        for (k, v) in &self.data.t {
            if k != SYSTEM_TOPIC_ROOT {
                children.insert(k.to_owned(), v.to_owned());
            }
        }
        children
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// retrieve a value for a non-wildcard key
    pub fn get(&self, path: &[RegularKeySegment]) -> Option<&Value> {
        let node = self.get_node(path)?;
        match node.v.as_ref()? {
            ValueEntry::Plain(value) => Some(value),
            ValueEntry::Cas(value, _) => Some(value),
        }
    }

    /// retrieve a value and its version for a non-wildcard key
    pub fn cget(&self, path: &[RegularKeySegment]) -> Option<(&Value, &CasVersion)> {
        let node = self.get_node(path)?;
        match node.v.as_ref()? {
            ValueEntry::Plain(value) => Some((value, &0)),
            ValueEntry::Cas(value, version) => Some((value, version)),
        }
    }

    fn get_node(&self, path: &[RegularKeySegment]) -> Option<&Node> {
        let mut current = &self.data;

        for elem in path {
            if let Some(node) = current.t.get(elem) {
                current = node;
            } else {
                return None;
            }
        }

        Some(current)
    }

    pub fn delete(
        &mut self,
        path: &[RegularKeySegment],
    ) -> WorterbuchResult<Option<(Value, Vec<AffectedLsSubscribers>)>> {
        let mut ls_subscribers = Vec::new();
        let removed = Store::ndelete(
            &mut self.data,
            path,
            Some(&self.subscribers),
            &mut ls_subscribers,
        )?
        .0;
        if removed.is_some() {
            self.len -= 1;
            self.unsaved_changes = true;
        }
        Ok(removed.map(|it| (it, ls_subscribers)))
    }

    /// retrieve values for a key containing at least one single-level wildcard and possibly a multi-level wildcard
    pub fn get_matches(&self, path: &[KeySegment]) -> StoreResult<Vec<KeyValuePair>> {
        let mut matches = Vec::new();
        let traversed = vec![];
        Store::ncollect_matches(
            &self.data,
            traversed,
            path,
            &mut matches,
            None,
            &mut Vec::new(),
        )?;
        Ok(matches)
    }

    pub fn delete_matches(
        &mut self,
        path: &[KeySegment],
    ) -> StoreResult<(Vec<KeyValuePair>, Vec<AffectedLsSubscribers>)> {
        let mut ls_subscribers = Vec::new();
        let mut matches = Vec::new();
        let traversed_path = vec![];
        Store::ndelete_matches(
            &mut self.data,
            traversed_path,
            &mut matches,
            path,
            Some(&self.subscribers),
            &mut ls_subscribers,
        )?;
        if self.len < matches.len() {
            self.len = 0;
        } else {
            self.len -= matches.len();
        }
        if !matches.is_empty() {
            self.unsaved_changes = true;
        }
        // TODO notify subscribers
        Ok((matches, ls_subscribers))
    }

    fn ndelete(
        node: &mut Node,
        relative_path: &[RegularKeySegment],
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> WorterbuchResult<(Option<Value>, CanDelete)> {
        if relative_path.is_empty() {
            match node.v.take() {
                Some(v) => match v {
                    ValueEntry::Plain(value) => return Ok((Some(value), node.t.is_empty())),
                    ValueEntry::Cas(value, _) => return Ok((Some(value), node.t.is_empty())),
                },
                None => return Ok((None, node.t.is_empty())),
            }
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        if let Entry::Occupied(mut e) = node.t.entry(head.to_owned()) {
            let next = e.get_mut();
            let (val, can_delete) = Store::ndelete(
                next,
                tail,
                subscribers.and_then(|s| s.tree.get(head)),
                ls_subscribers,
            )?;
            if can_delete {
                e.remove();
                let new_children: Vec<String> = node.t.keys().map(ToOwned::to_owned).collect();
                if let Some(subscribers) = subscribers.as_ref() {
                    if !subscribers.ls_subscribers.is_empty() {
                        let subscribers = subscribers.ls_subscribers.clone();
                        ls_subscribers.push((subscribers, new_children));
                    }
                }
            }
            Ok((val, node.v.is_none() && node.t.is_empty()))
        } else {
            Ok((None, node.v.is_none() && node.t.is_empty()))
        }
    }

    fn ndelete_matches(
        node: &mut Node,
        traversed_path: Vec<&str>,
        matches: &mut KeyValuePairs,
        relative_path: &[KeySegment],
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> StoreResult<CanDelete> {
        if relative_path.is_empty() {
            if let Some(value) = node.v.take() {
                let key = traversed_path.join("/");
                matches.push((key, value).into());
            }
            return Ok(node.t.is_empty());
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        match &head {
            KeySegment::MultiWildcard => {
                if !tail.is_empty() {
                    return Err(StoreError::IllegalMultiWildcard);
                }
                Store::ncollect_matches(
                    node,
                    traversed_path.clone(),
                    &[KeySegment::MultiWildcard],
                    matches,
                    subscribers,
                    ls_subscribers,
                )?;
                node.t.clear();
            }
            KeySegment::Wildcard => {
                for id in node
                    .t
                    .keys()
                    .map(ToOwned::to_owned)
                    .collect::<Vec<RegularKeySegment>>()
                {
                    let traversed_path = traversed_path.clone();
                    Store::ndelete_child_matches(
                        node,
                        &id,
                        traversed_path,
                        matches,
                        tail,
                        subscribers,
                        ls_subscribers,
                    )?;
                }
            }
            KeySegment::Regular(head) => {
                Store::ndelete_child_matches(
                    node,
                    head,
                    traversed_path,
                    matches,
                    tail,
                    subscribers,
                    ls_subscribers,
                )?;
            }
        }

        Ok(node.v.is_none() && node.t.is_empty())
    }

    fn ndelete_child_matches<'a>(
        node: &mut Node,
        id: &'a RegularKeySegment,
        mut traversed_path: Vec<&'a str>,
        matches: &mut KeyValuePairs,
        relative_path: &[KeySegment],
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> StoreResult<()> {
        traversed_path.push(id);
        if let Entry::Occupied(mut e) = node.t.entry(id.to_owned()) {
            let child = e.get_mut();
            let can_delete = Store::ndelete_matches(
                child,
                traversed_path,
                matches,
                relative_path,
                subscribers.and_then(|s| s.tree.get(id)),
                ls_subscribers,
            )?;
            if can_delete {
                e.remove();
                let new_children: Vec<String> = node.t.keys().map(ToOwned::to_owned).collect();
                if let Some(subscribers) = subscribers.as_ref() {
                    if !subscribers.ls_subscribers.is_empty() {
                        let subscribers = subscribers.ls_subscribers.clone();
                        ls_subscribers.push((subscribers, new_children));
                    }
                }
            }
        }

        Ok(())
    }

    fn ncollect_matches<'p>(
        node: &Node,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &'p [KeySegment],
        matches: &mut Vec<KeyValuePair>,
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> StoreResult<()> {
        if remaining_path.is_empty() {
            if let Some(value) = &node.v {
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

                if let Some(value) = &node.v {
                    let key = traversed_path.join("/");
                    matches.push((key, value.to_owned()).into());
                }

                for (key, node) in &node.t {
                    let new_children = Vec::new();
                    if let Some(subscribers) = subscribers.as_ref() {
                        if !subscribers.ls_subscribers.is_empty() {
                            let subscribers = subscribers.ls_subscribers.clone();
                            ls_subscribers.push((subscribers, new_children));
                        }
                    }
                    let mut traversed_path = traversed_path.clone();
                    traversed_path.push(key);
                    Store::ncollect_matches(
                        node,
                        traversed_path,
                        &[KeySegment::MultiWildcard],
                        matches,
                        subscribers.and_then(|s| s.tree.get(key)),
                        ls_subscribers,
                    )?;
                }
            }
            KeySegment::Wildcard => {
                for (key, node) in &node.t {
                    let new_children = Vec::new();
                    if let Some(subscribers) = subscribers.as_ref() {
                        if !subscribers.ls_subscribers.is_empty() {
                            let subscribers = subscribers.ls_subscribers.clone();
                            ls_subscribers.push((subscribers, new_children));
                        }
                    }
                    let mut traversed_path = traversed_path.clone();
                    traversed_path.push(key);
                    Store::ncollect_matches(
                        node,
                        traversed_path,
                        tail,
                        matches,
                        subscribers.and_then(|s| s.tree.get(key)),
                        ls_subscribers,
                    )?;
                }
            }
            KeySegment::Regular(elem) => {
                traversed_path.push(elem);
                if let Some(child) = node.t.get(elem) {
                    Store::ncollect_matches(
                        child,
                        traversed_path,
                        tail,
                        matches,
                        subscribers.and_then(|s| s.tree.get(elem)),
                        ls_subscribers,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn ncollect_matching_children<'p>(
        node: &Node,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &'p [KeySegment],
        children: &mut HashSet<RegularKeySegment>,
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> StoreResult<()> {
        if remaining_path.is_empty() {
            for key in node.t.keys() {
                children.insert(key.clone());
            }

            return Ok(());
        }

        let next = &remaining_path[0];
        let tail = &remaining_path[1..];

        match next {
            KeySegment::MultiWildcard => {
                return Err(StoreError::IllegalMultiWildcard);
            }
            KeySegment::Wildcard => {
                for (key, node) in &node.t {
                    let new_children = Vec::new();
                    if let Some(subscribers) = subscribers.as_ref() {
                        if !subscribers.ls_subscribers.is_empty() {
                            let subscribers = subscribers.ls_subscribers.clone();
                            ls_subscribers.push((subscribers, new_children));
                        }
                    }
                    let mut traversed_path = traversed_path.clone();
                    traversed_path.push(key);
                    Store::ncollect_matching_children(
                        node,
                        traversed_path,
                        tail,
                        children,
                        subscribers.and_then(|s| s.tree.get(key)),
                        ls_subscribers,
                    )?;
                }
            }
            KeySegment::Regular(elem) => {
                traversed_path.push(elem);
                if let Some(child) = node.t.get(elem) {
                    Store::ncollect_matching_children(
                        child,
                        traversed_path,
                        tail,
                        children,
                        subscribers.and_then(|s| s.tree.get(elem)),
                        ls_subscribers,
                    )?;
                }
            }
        }

        Ok(())
    }

    pub fn insert_plain(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
    ) -> WorterbuchResult<(bool, Vec<AffectedLsSubscribers>)> {
        self.insert(path, ValueEntry::Plain(value))
    }

    pub fn insert_cas(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
        version: u64,
    ) -> WorterbuchResult<(bool, Vec<AffectedLsSubscribers>)> {
        self.insert(path, ValueEntry::Cas(value, version))
    }

    fn insert(
        &mut self,
        path: &[RegularKeySegment],
        value: ValueEntry,
    ) -> WorterbuchResult<(bool, Vec<AffectedLsSubscribers>)> {
        let mut ls_subscribers = Vec::new();
        let changed = {
            let mut current_node = &mut self.data;
            let mut current_subscribers = Some(&self.subscribers);

            for (i, elem) in path.iter().enumerate() {
                current_node = match current_node.t.entry(elem.to_owned()) {
                    Entry::Occupied(e) => e.into_mut(),
                    Entry::Vacant(e) => {
                        if let Some(subscribers) = current_subscribers {
                            if !subscribers.ls_subscribers.is_empty() {
                                let subscribers = subscribers.ls_subscribers.clone();
                                ls_subscribers.push((subscribers, &path[0..i]));
                            }
                        }
                        e.insert(Node::default())
                    }
                };

                current_subscribers = current_subscribers.and_then(|node| node.tree.get(elem));
            }

            let mut value = value;

            let (inserted, changed) = if let Some(val) = &current_node.v {
                if let ValueEntry::Cas(new_val, prev_version) = value {
                    if let ValueEntry::Cas(_, version) = val {
                        if prev_version != *version {
                            return Err(WorterbuchError::CasVersionMismatch);
                        }
                    } else if prev_version != 0 {
                        return Err(WorterbuchError::CasVersionMismatch);
                    }
                    value = ValueEntry::Cas(new_val, prev_version + 1);
                }

                (false, val != &value)
            } else {
                if let ValueEntry::Cas(new_val, prev_version) = value {
                    if prev_version != 0 {
                        return Err(WorterbuchError::CasVersionMismatch);
                    }
                    value = ValueEntry::Cas(new_val, prev_version + 1);
                }

                (true, true)
            };

            current_node.v = Some(value);

            if inserted {
                self.len += 1;
            }

            changed
        };

        let ls_subscribers = ls_subscribers
            .into_iter()
            .filter_map(|(subscribers, path)| {
                {
                    if path.is_empty() {
                        Some(self.ls_root())
                    } else {
                        self.ls(path)
                    }
                }
                .map(|it| (subscribers, it))
            })
            .collect();

        if changed {
            self.unsaved_changes = true;
        }

        Ok((changed, ls_subscribers))
    }

    pub fn ls(&self, path: &[impl AsRef<str>]) -> Option<Vec<RegularKeySegment>> {
        if path.is_empty() {
            panic!("path must not be empty!");
        }
        let mut current = &self.data;

        for elem in path {
            current = current.t.get(elem.as_ref())?;
        }

        Some(current.t.keys().map(ToOwned::to_owned).collect())
    }

    pub fn ls_root(&self) -> Vec<RegularKeySegment> {
        self.data.t.keys().map(ToOwned::to_owned).collect()
    }

    pub fn pls(&self, path: &[KeySegment]) -> StoreResult<Vec<RegularKeySegment>> {
        let mut children = HashSet::new();
        let traversed = vec![];
        Store::ncollect_matching_children(
            &self.data,
            traversed,
            path,
            &mut children,
            None,
            &mut Vec::new(),
        )?;
        Ok(children.into_iter().collect())
    }

    pub fn merge(&mut self, other: Store) -> Vec<(String, Value)> {
        let mut insertions = Vec::new();
        let path = Vec::new();
        Store::nmerge(&mut self.data, other.data, None, &mut insertions, &path);
        self.len = Store::ncount_values(&self.data);
        self.unsaved_changes = true;
        // TODO notify subscribers
        insertions
    }

    pub fn count_entries(&mut self) {
        self.len = Store::ncount_values(&self.data);
    }

    pub fn count_sub_entries(&self, subkey: &str) -> WorterbuchResult<Option<usize>> {
        let path = parse_segments(subkey)?;
        let node = self.get_node(&path);
        Ok(node.map(Store::ncount_values))
    }

    fn nmerge(
        node: &mut Node,
        other: Node,
        key: Option<&str>,
        insertions: &mut Vec<(String, Value)>,
        path: &[&str],
    ) {
        if let Some(v) = other.v {
            node.v = Some(v.clone());
            let key = concat_key(path, key);
            log::debug!("Imported {} = {:?}", key, v);
            insertions.push((key, v.into()));
        }

        let mut path = path.to_owned();
        if let Some(key) = key {
            path.push(key);
        }

        for (key, other_node) in other.t {
            let own_node = match node.t.entry(key.clone()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(Node::default()),
            };
            Store::nmerge(own_node, other_node, Some(&key), insertions, &path);
        }
    }

    fn ncount_values(node: &Node) -> usize {
        let mut count = if node.v.is_some() { 1 } else { 0 };
        for child in node.t.values() {
            count += Store::ncount_values(child);
        }
        count
    }

    pub fn add_ls_subscriber(&mut self, parent: &[RegularKeySegment], subscriber: LsSubscriber) {
        log::debug!("Adding ls subscriber for parent {:?}", parent);
        let mut current = &mut self.subscribers;

        for elem in parent {
            current = match current.tree.entry(elem.to_owned()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(SubscribersNode::default()),
            };
        }

        current.ls_subscribers.push(subscriber);
    }

    pub fn remove_ls_subscriber(&mut self, subscriber: LsSubscriber) {
        let mut current = &mut self.subscribers;

        for elem in &subscriber.parent {
            if let Some(node) = current.tree.get_mut(elem) {
                current = node;
            } else {
                log::warn!("No ls subscriber found for parent {:?}", subscriber.parent);
                return;
            }
        }

        current.ls_subscribers.retain(|s| s.id != subscriber.id);
    }

    pub fn unsubscribe_ls(
        &mut self,
        parent: &[RegularKeySegment],
        subscription: &SubscriptionId,
    ) -> bool {
        let mut current = &mut self.subscribers;

        for elem in parent {
            if let Some(node) = current.tree.get_mut(elem) {
                current = node;
            } else {
                log::warn!("No ls subscriber found for pattern {:?}", parent);
                return false;
            }
        }
        let mut removed = false;
        current.ls_subscribers.retain(|s| {
            let retain = &s.id != subscription;
            removed = removed || !retain;
            if !retain {
                log::debug!("Removing subscription {subscription:?} to parent {parent:?}");
            }
            retain
        });
        if !removed {
            log::debug!("no matching subscription found")
        }
        removed
    }

    pub(crate) fn reset(&mut self, data: Node) {
        self.data = data;
        self.count_entries();
        self.unsaved_changes = true;
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
    use tokio::sync::mpsc;
    use uuid::Uuid;
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
        store.insert(&path, json!("Hello, World!").into()).unwrap();

        assert_eq!(store.get(&path), Some(&json!("Hello, World!")));
        assert_eq!(store.get(&reg_key_segs("test/a")), None);
        assert_eq!(store.get(&reg_key_segs("test/a/b/c")), None);
    }

    #[test]
    fn test_insert_delete() {
        let path = reg_key_segs("test/a/b");

        let mut store = Store::default();
        store.insert(&path, json!("Hello, World!").into()).unwrap();

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
        store.insert(&path0, json!("0").into()).unwrap();
        store.insert(&path1, json!("1").into()).unwrap();
        store.insert(&path2, json!("2").into()).unwrap();
        store.insert(&path3, json!("3").into()).unwrap();
        store.insert(&path4, json!("4").into()).unwrap();
        store.insert(&path5, json!("5").into()).unwrap();

        let res = store.get_matches(&key_segs("test/a/?")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .any(|e| e == &("test/a/b".to_owned(), json!("1")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("test/a/c".to_owned(), json!("2")).into()));

        let res = store.get_matches(&key_segs("trolo/?/b")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .any(|e| e == &("trolo/a/b".to_owned(), json!("3")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/c/b".to_owned(), json!("4")).into()));

        let res = store.get_matches(&key_segs("?/a/b")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .any(|e| e == &("test/a/b".to_owned(), json!("1")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/a/b".to_owned(), json!("3")).into()));

        let res = store.get_matches(&key_segs("?/?/b")).unwrap();
        assert_eq!(res.len(), 3);
        assert!(res
            .iter()
            .any(|e| e == &("test/a/b".to_owned(), json!("1")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/a/b".to_owned(), json!("3")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/c/b".to_owned(), json!("4")).into()));
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
        store.insert(&path0, json!("0").into()).unwrap();
        store.insert(&path1, json!("1").into()).unwrap();
        store.insert(&path2, json!("2").into()).unwrap();
        store.insert(&path3, json!("3").into()).unwrap();
        store.insert(&path4, json!("4").into()).unwrap();
        store.insert(&path5, json!("5").into()).unwrap();

        let res = store.get_matches(&key_segs("test/a/#")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .any(|e| e == &("test/a/b".to_owned(), json!("1")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("test/a/c".to_owned(), json!("2")).into()));

        let res = store.get_matches(&key_segs("trolo/#")).unwrap();
        assert_eq!(res.len(), 4);
        assert!(res
            .iter()
            .any(|e| e == &("trolo/a".to_owned(), json!("0")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/a/b".to_owned(), json!("3")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/c/b".to_owned(), json!("4")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/c/b/d".to_owned(), json!("5")).into()));

        let res = store.get_matches(&key_segs("#")).unwrap();
        assert_eq!(res.len(), 6);
        assert!(res
            .iter()
            .any(|e| e == &("trolo/a".to_owned(), json!("0")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("test/a/b".to_owned(), json!("1")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("test/a/c".to_owned(), json!("2")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/a/b".to_owned(), json!("3")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/c/b".to_owned(), json!("4")).into()));
        assert!(res
            .iter()
            .any(|e| e == &("trolo/c/b/d".to_owned(), json!("5")).into()));
    }

    #[test]
    fn insert_detects_ls_subscribers_empty() {
        let mut store = Store::default();
        let parent = ["hello".to_owned(), "there".to_owned()];
        let path = ["hello".to_owned(), "there".to_owned(), "world".to_owned()];

        let (_, subscribers) = store.insert(&path, json!("Hello!").into()).unwrap();
        assert!(subscribers.is_empty());

        let (tx, _) = mpsc::channel(1);
        let subscriber = LsSubscriber::new(
            SubscriptionId::new(Uuid::new_v4(), 123),
            parent.clone().into(),
            tx,
        );
        store.add_ls_subscriber(&parent, subscriber);
        let (_, subscribers) = store.insert(&path, json!("Hello There!").into()).unwrap();
        assert!(subscribers.is_empty());
    }

    #[test]
    fn insert_detects_ls_subscribers_not_empty() {
        let mut store = Store::default();
        let parent = ["hello".to_owned(), "there".to_owned()];
        let path = ["hello".to_owned(), "there".to_owned(), "world".to_owned()];

        let (tx, _) = mpsc::channel(1);
        let subscriber = LsSubscriber::new(
            SubscriptionId::new(Uuid::new_v4(), 123),
            parent.clone().into(),
            tx,
        );
        store.add_ls_subscriber(&parent, subscriber);
        let (_, subscribers) = store.insert(&path, json!("Hello There!").into()).unwrap();
        assert_eq!(subscribers.len(), 1);
        assert_eq!(subscribers[0].1, vec!["world".to_owned()]);
    }

    #[test]
    fn initial_cas_insert_with_version_0_succeeds() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_cas(&path, json!("hello"), 0).unwrap();
    }

    #[test]
    fn plain_to_cas_upgrade_with_version_0_succeeds() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_plain(&path, json!(1)).unwrap();
        store.insert_cas(&path, json!(2), 0).unwrap();
    }

    #[test]
    fn initial_cas_insert_with_version_1_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!("hello"), 1).is_err())
    }

    #[test]
    fn plain_to_cas_upgrade_with_version_1_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_plain(&path, json!(1)).unwrap();
        assert!(store.insert_cas(&path, json!(2), 1).is_err())
    }

    #[test]
    fn cas_set_with_previous_version_number_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_cas(&path, json!(0), 0).unwrap();
        store.insert_cas(&path, json!(1), 1).unwrap();
        store.insert_cas(&path, json!(2), 2).unwrap();
        assert!(store.insert_cas(&path, json!(3), 1).is_err())
    }

    #[test]
    fn cas_set_with_same_version_number_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_cas(&path, json!(0), 0).unwrap();
        store.insert_cas(&path, json!(1), 1).unwrap();
        store.insert_cas(&path, json!(2), 2).unwrap();
        assert!(store.insert_cas(&path, json!(3), 2).is_err())
    }

    #[test]
    fn cas_set_with_next_version_number_succeeds() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_cas(&path, json!(0), 0).unwrap();
        store.insert_cas(&path, json!(1), 1).unwrap();
        store.insert_cas(&path, json!(2), 2).unwrap();
        store.insert_cas(&path, json!(3), 3).unwrap();
    }

    #[test]
    fn cas_set_with_future_version_number_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_cas(&path, json!(0), 0).unwrap();
        store.insert_cas(&path, json!(1), 1).unwrap();
        store.insert_cas(&path, json!(2), 2).unwrap();
        assert!(store.insert_cas(&path, json!(3), 4).is_err())
    }

    #[test]
    fn cas_value_can_be_deleted() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_cas(&path, json!(0), 0).unwrap();
        assert_eq!(store.get(&path), Some(&json!(0)));
        store.delete(&path).unwrap();
        assert_eq!(store.get(&path), None);
    }

    #[test]
    fn cas_value_can_be_pdeleted() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let pattern = key_segs("hello/#");
        store.insert_cas(&path, json!(0), 0).unwrap();
        assert_eq!(store.get(&path), Some(&json!(0)));
        store.delete_matches(&pattern).unwrap();
        assert_eq!(store.get(&path), None);
    }

    #[test]
    fn cas_set_increments_version() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        store.insert_cas(&path, json!(0), 0).unwrap();
        assert_eq!(store.cget(&path), Some((&json!(0), &1)));
        store.insert_cas(&path, json!(1), 1).unwrap();
        assert_eq!(store.cget(&path), Some((&json!(1), &2)));
    }
}
