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

use crate::subscribers::{LsSubscriber, Subscriber, SubscriptionId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};
use tokio::sync::oneshot;
use tracing::{Level, debug, instrument, warn};
use uuid::Uuid;
use worterbuch_common::{
    CasVersion, KeySegment, KeyValuePair, KeyValuePairs, RegularKeySegment, SYSTEM_TOPIC_ROOT,
    Value,
    error::{WorterbuchError, WorterbuchResult},
    parse_segments,
};

type Tree<V> = HashMap<RegularKeySegment, Node<V>>;
type SubscribersTree = HashMap<RegularKeySegment, SubscribersNode>;
type CanDelete = bool;

pub type AffectedLsSubscribers = (Vec<LsSubscriber>, Vec<RegularKeySegment>);

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedStore {
    pub data: Node<ValueEntry>,
}

impl From<PersistedStore> for Store {
    fn from(value: PersistedStore) -> Self {
        let mut store = Store {
            data: value.data,
            ..Default::default()
        };
        store.count_entries();
        store
    }
}

struct Lock {
    holder: Uuid,
    candidates: VecDeque<(Uuid, Vec<oneshot::Sender<()>>)>,
}

impl Lock {
    fn new(client_id: Uuid) -> Self {
        Lock {
            holder: client_id,
            candidates: VecDeque::new(),
        }
    }

    async fn release(&mut self, client_id: Uuid) -> (bool, bool) {
        if client_id == self.holder {
            if let Some((id, txs)) = self.candidates.pop_front() {
                self.holder = id;
                for tx in txs {
                    tx.send(()).ok();
                }
                (true, false)
            } else {
                (true, true)
            }
        } else {
            self.candidates.retain(|(c, _)| c != &client_id);
            (false, false)
        }
    }

    async fn queue(&mut self, client_id: Uuid, tx: oneshot::Sender<()>) {
        if let Some((_, txs)) = self.candidates.iter_mut().find(|(id, _)| id == &client_id) {
            txs.push(tx);
        } else {
            self.candidates.push_back((client_id, vec![tx]));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValueEntry {
    Cas(Value, u64),
    #[serde(untagged)]
    Plain(Value),
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
pub struct Node<V> {
    #[serde(rename = "v")]
    #[serde(skip_serializing_if = "Option::is_none")]
    v: Option<V>,
    #[serde(rename = "t")]
    #[serde(skip_serializing_if = "Tree::is_empty", default = "Tree::default")]
    t: Tree<V>,
}

impl Default for Node<ValueEntry> {
    fn default() -> Self {
        Self {
            v: Default::default(),
            t: Default::default(),
        }
    }
}

impl Default for Node<Lock> {
    fn default() -> Self {
        Self {
            v: Default::default(),
            t: Default::default(),
        }
    }
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

#[derive(Default)]
pub struct Store {
    data: Node<ValueEntry>,
    len: usize,
    subscribers: SubscribersNode,
    locked_keys: HashMap<Uuid, Vec<Box<[RegularKeySegment]>>>,
    locks: Node<Lock>,
}

impl Store {
    pub fn with_data(data: Node<ValueEntry>) -> Self {
        let mut store = Self {
            data,
            ..Default::default()
        };

        store.count_entries();

        store
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    pub fn export(&mut self) -> Node<ValueEntry> {
        debug!("Exporting slim copy of store with {} entries …", self.len);
        let data = Node {
            v: self.data.v.clone(),
            t: self.slim_copy_top_level_children(),
        };
        debug!(
            "Exported slim copy with {} entries.",
            Store::ncount_values(&data)
        );
        data
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    pub fn export_for_persistence(&mut self) -> PersistedStore {
        let data = self.export();
        PersistedStore { data }
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    fn slim_copy_top_level_children(&self) -> Tree<ValueEntry> {
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

    fn get_node(&self, path: &[RegularKeySegment]) -> Option<&Node<ValueEntry>> {
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

    fn get_or_create_lock_node(&mut self, path: Box<[RegularKeySegment]>) -> &mut Node<Lock> {
        let mut current = &mut self.locks;

        for elem in path {
            current = match current.t.entry(elem) {
                Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
                Entry::Vacant(vacant_entry) => vacant_entry.insert(Node::default()),
            }
        }

        current
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
        }
        Ok(removed.map(|it| (it, ls_subscribers)))
    }

    pub fn delete_lock_node(&mut self, path: &[RegularKeySegment]) {
        Store::ndelete_lock_nodes(&mut self.locks, path);
    }

    fn ndelete_lock_nodes(node: &mut Node<Lock>, relative_path: &[RegularKeySegment]) -> CanDelete {
        if relative_path.is_empty() {
            node.v.take();
            return node.t.is_empty();
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        if let Entry::Occupied(mut e) = node.t.entry(head.to_owned()) {
            let next = e.get_mut();
            let can_delete = Store::ndelete_lock_nodes(next, tail);
            if can_delete {
                e.remove();
            }
        }
        node.v.is_none() && node.t.is_empty()
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
        // TODO notify subscribers
        Ok((matches, ls_subscribers))
    }

    fn ndelete(
        node: &mut Node<ValueEntry>,
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
                if let Some(subscribers) = subscribers.as_ref()
                    && !subscribers.ls_subscribers.is_empty()
                {
                    let subscribers = subscribers.ls_subscribers.clone();
                    ls_subscribers.push((subscribers, new_children));
                }
            }
            Ok((val, node.v.is_none() && node.t.is_empty()))
        } else {
            Ok((None, node.v.is_none() && node.t.is_empty()))
        }
    }

    fn ndelete_matches(
        node: &mut Node<ValueEntry>,
        traversed_path: Vec<&str>,
        matches: &mut KeyValuePairs,
        relative_path: &[KeySegment],
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> StoreResult<CanDelete> {
        if relative_path.is_empty() {
            if let Some(value) = node.v.take() {
                let key = traversed_path.join("/");
                let value = match value {
                    ValueEntry::Plain(it) => it,
                    ValueEntry::Cas(it, _) => it,
                };
                matches.push(KeyValuePair::new(key, value));
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
        node: &mut Node<ValueEntry>,
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
                if let Some(subscribers) = subscribers.as_ref()
                    && !subscribers.ls_subscribers.is_empty()
                {
                    let subscribers = subscribers.ls_subscribers.clone();
                    ls_subscribers.push((subscribers, new_children));
                }
            }
        }

        Ok(())
    }

    fn ncollect_matches<'p>(
        node: &Node<ValueEntry>,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &'p [KeySegment],
        matches: &mut Vec<KeyValuePair>,
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> StoreResult<()> {
        if remaining_path.is_empty() {
            if let Some(value) = &node.v {
                let key = traversed_path.join("/");
                let value = match value {
                    ValueEntry::Plain(it) => it,
                    ValueEntry::Cas(it, _) => it,
                };
                matches.push(KeyValuePair::new(key, value.to_owned()));
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
                    let value = match value {
                        ValueEntry::Plain(it) => it,
                        ValueEntry::Cas(it, _) => it,
                    };
                    matches.push(KeyValuePair::new(key, value.to_owned()));
                }

                for (key, node) in &node.t {
                    let new_children = Vec::new();
                    if let Some(subscribers) = subscribers.as_ref()
                        && !subscribers.ls_subscribers.is_empty()
                    {
                        let subscribers = subscribers.ls_subscribers.clone();
                        ls_subscribers.push((subscribers, new_children));
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
                    if let Some(subscribers) = subscribers.as_ref()
                        && !subscribers.ls_subscribers.is_empty()
                    {
                        let subscribers = subscribers.ls_subscribers.clone();
                        ls_subscribers.push((subscribers, new_children));
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
        node: &Node<ValueEntry>,
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
                    if let Some(subscribers) = subscribers.as_ref()
                        && !subscribers.ls_subscribers.is_empty()
                    {
                        let subscribers = subscribers.ls_subscribers.clone();
                        ls_subscribers.push((subscribers, new_children));
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
        let mut current_node = &mut self.data;
        let mut current_subscribers = Some(&self.subscribers);

        for (i, elem) in path.iter().enumerate() {
            current_node = match current_node.t.entry(elem.to_owned()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    if let Some(subscribers) = current_subscribers
                        && !subscribers.ls_subscribers.is_empty()
                    {
                        let subscribers = subscribers.ls_subscribers.clone();
                        ls_subscribers.push((subscribers, &path[0..i]));
                    }
                    e.insert(Node::default())
                }
            };

            current_subscribers = current_subscribers.and_then(|node| node.tree.get(elem));
        }

        let mut value = value;

        let (inserted, changed) = if let Some(val) = &current_node.v {
            // value already exists

            if let ValueEntry::Cas(_, version) = val {
                // existing value is cas

                if let ValueEntry::Cas(new_val, prev_version) = value {
                    if prev_version != *version {
                        return Err(WorterbuchError::CasVersionMismatch);
                    }
                    value = ValueEntry::Cas(new_val, prev_version + 1);
                } else {
                    return Err(WorterbuchError::Cas);
                }
            } else {
                // existing value is plain

                if let ValueEntry::Cas(new_val, prev_version) = value {
                    if prev_version != 0 {
                        return Err(WorterbuchError::CasVersionMismatch);
                    }
                    value = ValueEntry::Cas(new_val, 1);
                }
            }

            (false, val != &value)
        } else {
            // value doesn't exist yet

            if let ValueEntry::Cas(new_val, prev_version) = value {
                if prev_version != 0 {
                    return Err(WorterbuchError::CasVersionMismatch);
                }
                value = ValueEntry::Cas(new_val, 1);
            }

            (true, true)
        };

        current_node.v = Some(value);

        if inserted {
            self.len += 1;
        }

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

    pub fn merge(&mut self, other: Node<ValueEntry>) -> Vec<(String, (ValueEntry, bool))> {
        let mut insertions = Vec::new();
        let path = Vec::new();
        Store::nmerge(&mut self.data, other, None, &mut insertions, &path);
        self.len = Store::ncount_values(&self.data);
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
        node: &mut Node<ValueEntry>,
        other: Node<ValueEntry>,
        key: Option<&str>,
        insertions: &mut Vec<(String, (ValueEntry, bool))>,
        path: &[&str],
    ) {
        if let Some(v) = other.v {
            let changed = node.v.as_ref() != Some(&v);
            node.v = Some(v.clone());
            let key = concat_key(path, key);
            debug!("Imported {} = {:?}", key, v);
            insertions.push((key, (v, changed)));
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

    fn ncount_values<V>(node: &Node<V>) -> usize {
        let mut count = if node.v.is_some() { 1 } else { 0 };
        for child in node.t.values() {
            count += Store::ncount_values(child);
        }
        count
    }

    pub fn add_ls_subscriber(&mut self, parent: &[RegularKeySegment], subscriber: LsSubscriber) {
        debug!("Adding ls subscriber for parent {:?}", parent);
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
                warn!("No ls subscriber found for parent {:?}", subscriber.parent);
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
                warn!("No ls subscriber found for pattern {:?}", parent);
                return false;
            }
        }
        let mut removed = false;
        current.ls_subscribers.retain(|s| {
            let retain = &s.id != subscription;
            removed = removed || !retain;
            if !retain {
                debug!("Removing subscription {subscription:?} to parent {parent:?}");
            }
            retain
        });
        if !removed {
            debug!("no matching subscription found")
        }
        removed
    }

    pub fn lock(
        &mut self,
        client_id: Uuid,
        path: Box<[RegularKeySegment]>,
    ) -> WorterbuchResult<()> {
        let node = self.get_or_create_lock_node(path.clone());
        match &mut node.v {
            Some(lock) => {
                if client_id == lock.holder {
                    debug!("Client {client_id} already holds the lock on {path:?}");
                } else {
                    return Err(WorterbuchError::KeyIsLocked(path.join("/")));
                }
            }
            None => {
                node.v = Some(Lock::new(client_id));
                let paths = match self.locked_keys.entry(client_id) {
                    Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
                    Entry::Vacant(vacant_entry) => vacant_entry.insert(Vec::new()),
                };
                paths.push(path);
            }
        }

        Ok(())
    }

    pub async fn acquire_lock(
        &mut self,
        client_id: Uuid,
        path: Box<[RegularKeySegment]>,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let node = self.get_or_create_lock_node(path.clone());
        match &mut node.v {
            Some(lock) => {
                if client_id == lock.holder {
                    debug!("Client {client_id} already holds the lock on {path:?}");
                    tx.send(()).ok();
                } else {
                    lock.queue(client_id, tx).await;
                }
            }
            None => {
                node.v = Some(Lock::new(client_id));
                tx.send(()).ok();
            }
        }

        let paths = match self.locked_keys.entry(client_id) {
            Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
            Entry::Vacant(vacant_entry) => vacant_entry.insert(Vec::new()),
        };
        paths.push(path);

        rx
    }

    pub async fn unlock_all(&mut self, client_id: Uuid) {
        if let Some(paths) = self.locked_keys.remove(&client_id) {
            for path in paths {
                self.unlock(client_id, path).await.ok();
            }
        }
    }

    pub async fn unlock(
        &mut self,
        client_id: Uuid,
        path: Box<[RegularKeySegment]>,
    ) -> WorterbuchResult<()> {
        let node = self.get_or_create_lock_node(path.clone());

        if let Some(lock) = &mut node.v {
            let (was_holder, is_now_unlocked) = lock.release(client_id).await;
            if !was_holder {
                return Err(WorterbuchError::KeyIsLocked(path.join("/")));
            } else if is_now_unlocked {
                self.delete_lock_node(&path);
            }
        } else {
            warn!("Node {path:?} is not locked.");
            return Err(WorterbuchError::KeyIsNotLocked(path.join("/")));
        }

        Ok(())
    }

    pub(crate) fn reset(&mut self, data: Node<ValueEntry>) {
        self.data = data;
        self.count_entries();
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

    fn reg_key_segs(key: &str) -> Box<[RegularKeySegment]> {
        parse_segments(key).unwrap().into()
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
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/b".to_owned(), json!("1")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/c".to_owned(), json!("2")))
        );

        let res = store.get_matches(&key_segs("trolo/?/b")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/a/b".to_owned(), json!("3")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/c/b".to_owned(), json!("4")))
        );

        let res = store.get_matches(&key_segs("?/a/b")).unwrap();
        assert_eq!(res.len(), 2);
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/b".to_owned(), json!("1")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/a/b".to_owned(), json!("3")))
        );

        let res = store.get_matches(&key_segs("?/?/b")).unwrap();
        assert_eq!(res.len(), 3);
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/b".to_owned(), json!("1")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/a/b".to_owned(), json!("3")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/c/b".to_owned(), json!("4")))
        );
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
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/b".to_owned(), json!("1")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/c".to_owned(), json!("2")))
        );

        let res = store.get_matches(&key_segs("trolo/#")).unwrap();
        assert_eq!(res.len(), 4);
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/a".to_owned(), json!("0")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/a/b".to_owned(), json!("3")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/c/b".to_owned(), json!("4")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/c/b/d".to_owned(), json!("5")))
        );

        let res = store.get_matches(&key_segs("#")).unwrap();
        assert_eq!(res.len(), 6);
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/a".to_owned(), json!("0")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/b".to_owned(), json!("1")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("test/a/c".to_owned(), json!("2")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/a/b".to_owned(), json!("3")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/c/b".to_owned(), json!("4")))
        );
        assert!(
            res.iter()
                .any(|e| e == &KeyValuePair::of("trolo/c/b/d".to_owned(), json!("5")))
        );
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
        assert!(store.insert_cas(&path, json!("hello"), 0).is_ok());
    }

    #[test]
    fn plain_to_cas_upgrade_with_version_0_succeeds() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_plain(&path, json!(1)).is_ok());
        assert!(store.insert_cas(&path, json!(2), 0).is_ok());
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
        assert!(store.insert_plain(&path, json!(1)).is_ok());
        assert!(store.insert_cas(&path, json!(2), 1).is_err());
    }

    #[test]
    fn cas_set_with_previous_version_number_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert!(store.insert_cas(&path, json!(1), 1).is_ok());
        assert!(store.insert_cas(&path, json!(2), 2).is_ok());
        assert!(store.insert_cas(&path, json!(3), 1).is_err());
    }

    #[test]
    fn cas_set_with_same_version_number_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert!(store.insert_cas(&path, json!(1), 1).is_ok());
        assert!(store.insert_cas(&path, json!(2), 2).is_ok());
        assert!(store.insert_cas(&path, json!(3), 2).is_err());
    }

    #[test]
    fn cas_set_with_next_version_number_succeeds() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert!(store.insert_cas(&path, json!(1), 1).is_ok());
        assert!(store.insert_cas(&path, json!(2), 2).is_ok());
        assert!(store.insert_cas(&path, json!(3), 3).is_ok());
    }

    #[test]
    fn cas_set_with_future_version_number_fails() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert!(store.insert_cas(&path, json!(1), 1).is_ok());
        assert!(store.insert_cas(&path, json!(2), 2).is_ok());
        assert!(store.insert_cas(&path, json!(3), 4).is_err());
    }

    #[test]
    fn cas_value_can_be_deleted() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert_eq!(store.get(&path), Some(&json!(0)));
        assert!(store.delete(&path,).is_ok());
        assert_eq!(store.get(&path), None);
    }

    #[test]
    fn cas_value_can_be_pdeleted() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let pattern = key_segs("hello/#");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert_eq!(store.get(&path), Some(&json!(0)));
        assert!(store.delete_matches(&pattern).is_ok());
        assert_eq!(store.get(&path), None);
    }

    #[test]
    fn cas_set_increments_version() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert_eq!(store.cget(&path), Some((&json!(0), &1)));
        assert!(store.insert_cas(&path, json!(1), 1).is_ok());
        assert_eq!(store.cget(&path), Some((&json!(1), &2)));
    }

    #[test]
    fn regular_set_cannot_overwrite_cas_value() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert_eq!(store.cget(&path), Some((&json!(0), &1)));
        assert!(store.insert_plain(&path, json!(1)).is_err());
    }

    #[test]
    fn lock_can_be_acquired() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let client = Uuid::new_v4();
        assert!(store.lock(client, path).is_ok());
    }

    #[test]
    fn lock_can_be_acquired_by_only_one_client() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let client_1 = Uuid::new_v4();
        let client_2 = Uuid::new_v4();
        assert!(store.lock(client_1, path.clone()).is_ok());
        assert!(store.lock(client_2, path).is_err());
    }

    #[tokio::test]
    async fn lock_can_be_re_acquired_after_unlock() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let client_1 = Uuid::new_v4();
        let client_2 = Uuid::new_v4();
        assert!(store.lock(client_1, path.clone()).is_ok());
        assert!(store.lock(client_2, path.clone()).is_err());
        store.unlock(client_1, path.clone()).await.unwrap();
        assert!(store.lock(client_2, path).is_ok());
    }

    #[test]
    fn locked_value_can_be_written() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let client_1 = Uuid::new_v4();
        assert!(store.lock(client_1, path.clone()).is_ok());
        assert!(store.insert_plain(&path, json!("hello")).is_ok());
        assert_eq!(store.get(&path), Some(&json!("hello")));
    }

    #[test]
    fn locked_value_can_be_deleted() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let client_1 = Uuid::new_v4();
        assert!(store.lock(client_1, path.clone()).is_ok());
        assert!(store.insert_plain(&path, json!("hello")).is_ok());
        assert_eq!(store.get(&path), Some(&json!("hello")));
        assert!(store.delete(&path,).is_ok());
        assert_eq!(store.get(&path), None);
    }

    #[tokio::test]
    async fn unlocking_a_key_does_not_delete_its_value() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let client_1 = Uuid::new_v4();
        assert!(store.lock(client_1, path.clone()).is_ok());
        assert!(store.insert_plain(&path, json!("hello")).is_ok());
        store.unlock(client_1, path.clone()).await.unwrap();
        assert_eq!(store.get(&path), Some(&json!("hello")));
    }

    #[tokio::test]
    async fn unlocking_a_key_does_not_delete_its_sub_tree() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        let path2 = reg_key_segs("hello/cas/test");
        let client_1 = Uuid::new_v4();
        assert!(store.lock(client_1, path.clone()).is_ok());
        assert!(store.insert_plain(&path2, json!("hello")).is_ok());
        store.unlock(client_1, path.clone()).await.unwrap();
        assert_eq!(store.get(&path2), Some(&json!("hello")));
    }

    #[test]
    fn locked_key_is_deleted_by_pdelete() {
        let mut store = Store::default();

        let del = key_segs("hello/#");
        let path = reg_key_segs("hello/cas");
        let path2 = reg_key_segs("hello/cas/test");
        let path3 = reg_key_segs("hello/cas/test2");
        let client_1 = Uuid::new_v4();
        store.insert_plain(&path, json!("hello1")).unwrap();
        store.insert_plain(&path2, json!("hello2")).unwrap();
        store.insert_plain(&path3, json!("hello3")).unwrap();
        store.lock(client_1, path2.clone()).unwrap();
        assert!(store.delete_matches(&del).is_ok());
        assert_eq!(store.get(&path), None);
        assert_eq!(store.get(&path2), None);
        assert_eq!(store.get(&path3), None);
    }

    #[test]
    fn cas_values_are_serde_roundtrippable() {
        let val = ValueEntry::Cas(json!(123), 1);

        // Value
        let json = json!(val);
        let deserialized = serde_json::from_value(json).unwrap();
        assert_eq!(val, deserialized);

        // String
        let json = json!(val).to_string();
        let deserialized = serde_json::from_str(&json).unwrap();
        assert_eq!(val, deserialized);
    }
}
