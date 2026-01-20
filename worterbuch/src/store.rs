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

use crate::subscribers::{LsSubscriber, Subscriber};
use hashbrown::{
    HashMap, HashSet,
    hash_map::{Entry, Keys},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::VecDeque,
    hash::Hash,
    marker::PhantomData,
    mem::{self},
};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{Level, debug, instrument, warn};
use uuid::Uuid;
use worterbuch_common::{
    CasVersion, KeySegment, KeyValuePair, KeyValuePairs, RegularKeySegment, SYSTEM_TOPIC_ROOT,
    SubscriptionId, Value, ValueEntry,
    error::{WorterbuchError, WorterbuchResult},
    format_path,
};

type Tree<K, V> = HashMap<RegularKeySegment, Node<K, V>>;
type SubscribersTree = HashMap<RegularKeySegment, SubscribersNode>;

pub type AffectedLsSubscribers = (Vec<LsSubscriber>, Vec<RegularKeySegment>);

pub type StoreNode = Node<RegularKeySegment, ValueEntry>;
type LockNode = Node<RegularKeySegment, Lock>;

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedStore {
    pub data: StoreNode,
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

    async fn release(&mut self, client_id: Uuid) -> (bool, Option<Uuid>) {
        if client_id == self.holder {
            if let Some((id, txs)) = self.candidates.pop_front() {
                self.holder = id;
                for tx in txs {
                    tx.send(()).ok();
                }
                (true, Some(self.holder))
            } else {
                (true, None)
            }
        } else {
            self.candidates.retain(|(c, _)| c != &client_id);
            (false, Some(self.holder))
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

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Illegal multi-level wildcard in key: {0}")]
    IllegalMultiWildcard(String),
    #[error("Could not serialize/deserialize JSON: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("CAS version mismatch")]
    CasVersionMismatch,
    #[error("Value is CAS protected")]
    Cas,
}

impl From<StoreError> for WorterbuchError {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::IllegalMultiWildcard(pattern) => {
                WorterbuchError::IllegalMultiWildcard(pattern)
            }
            StoreError::CasVersionMismatch => WorterbuchError::CasVersionMismatch,
            StoreError::Cas => WorterbuchError::Cas,
            StoreError::SerdeJsonError(e) => WorterbuchError::SerDeError(
                e,
                "Could not serialize/deserialize JSON from/to persistence".to_owned(),
            ),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node<K, V>
where
    K: Eq + Hash,
{
    #[serde(rename = "v")]
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<V>,
    #[serde(rename = "t")]
    #[serde(skip_serializing_if = "Option::is_none")]
    tree: Option<Tree<K, V>>,
    #[serde(skip, default)]
    _key_type: PhantomData<K>,
}

impl<K, V> Node<K, V>
where
    K: DeserializeOwned + Eq + Hash,
{
    fn into_sub_tree(self) -> Option<Tree<K, V>> {
        self.tree
    }

    fn sub_tree(&self) -> Option<&Tree<K, V>> {
        self.tree.as_ref()
    }

    fn strip(&mut self) {
        if let Some(t) = &mut self.tree {
            t.remove(SYSTEM_TOPIC_ROOT);
        }
    }

    fn get_child(&self, key: impl AsRef<str>) -> Option<&Node<K, V>> {
        self.tree.as_ref()?.get(key.as_ref())
    }

    fn get_child_mut(&mut self, key: impl AsRef<str>) -> Option<&mut Node<K, V>> {
        self.tree.as_mut()?.get_mut(key.as_ref())
    }

    fn get_or_create_child(&mut self, key: RegularKeySegment) -> (&mut Node<K, V>, bool) {
        if self.tree.is_none() {
            self.tree = Some(Tree::new());
        }

        match self.tree.as_mut().expect("connot be None").entry(key) {
            Entry::Occupied(e) => (e.into_mut(), false),
            Entry::Vacant(e) => (e.insert(Node::default()), true),
        }
    }

    fn ls<'a>(&'a self) -> Option<Keys<'a, RegularKeySegment, Node<K, V>>> {
        self.tree.as_ref().map(|t| t.keys())
    }

    fn ls_owned(&self) -> Vec<RegularKeySegment> {
        if let Some(keys) = self.ls() {
            keys.map(ToOwned::to_owned).collect()
        } else {
            vec![]
        }
    }

    /// Removes any obsolete children
    fn trim(&mut self) -> bool {
        let mut removed = false;
        if let Some(t) = self.tree.as_mut() {
            t.retain(|_, child| {
                let obsolete = child.is_obsolete();
                removed |= obsolete;
                !obsolete
            });
            if t.is_empty() {
                self.tree = None;
            }
        }
        removed
    }

    /// Test if a node is obsolete. A leaf node is obsolete, if it has no value, an internal node is obsolete if it has no value and no children.
    /// Note that this does not recursively check children for obsolescence, for a recursive check use `is_clean()`
    fn is_obsolete(&self) -> bool {
        self.value.is_none() && self.is_empty()
    }

    fn is_empty(&self) -> bool {
        self.tree.as_ref().map(Tree::is_empty).unwrap_or(true)
    }

    /// Recursively check if this node's subtree contains any obsolete children
    fn is_clean(&self) -> bool {
        match self.tree.as_ref() {
            None => self.value.is_some(),
            Some(t) => {
                if self.value.is_none() && t.is_empty() {
                    false
                } else {
                    t.iter().all(|(_, v)| v.is_clean())
                }
            }
        }
    }

    fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }

    fn value_mut(&mut self) -> Option<&mut V> {
        self.value.as_mut()
    }

    fn take_value(&mut self) -> Option<V> {
        self.value.take()
    }

    fn set_value(&mut self, value: V) {
        self.value = Some(value)
    }

    fn drop_children(&mut self) {
        self.tree = None
    }
}

impl<K, T> Default for Node<K, T>
where
    K: DeserializeOwned + Eq + Hash,
{
    fn default() -> Self {
        Self {
            value: None,
            tree: None,
            _key_type: PhantomData,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubscribersNode {
    _subscribers: Vec<Subscriber>,
    ls_subscribers: Vec<LsSubscriber>,
    pub tree: SubscribersTree,
}

#[derive(Default)]
pub struct Store {
    data: StoreNode,
    len: usize,
    subscribers: SubscribersNode,
    locked_keys: HashMap<Uuid, Vec<Box<[RegularKeySegment]>>>,
    locks: LockNode,
}

impl Store {
    #[instrument(level=Level::DEBUG, skip(self))]
    pub fn export(&mut self) -> StoreNode {
        debug!("Exporting slim copy of store with {} entries …", self.len);
        let data_copy = self.data.clone();
        let mut original_data = mem::replace(&mut self.data, data_copy);
        original_data.strip();
        debug!(
            "Exported slim copy with {} entries.",
            Store::ncount_values(&original_data)
        );
        original_data
    }

    #[instrument(level=Level::DEBUG, skip(self))]
    pub fn export_for_persistence(&mut self) -> PersistedStore {
        let data = self.export();
        PersistedStore { data }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// retrieve a value for a non-wildcard key
    pub fn get(&self, path: &[RegularKeySegment]) -> Option<&Value> {
        self.get_node(path)?.value().map(AsRef::as_ref)
    }

    /// retrieve a value and its version for a non-wildcard key
    pub fn cget(&self, path: &[RegularKeySegment]) -> Option<(&Value, CasVersion)> {
        match self.get_node(path)?.value()? {
            ValueEntry::Cas(value, version) => Some((value, *version)),
            ValueEntry::Plain(value) => Some((value, 0)),
        }
    }

    fn get_node(&self, path: &[RegularKeySegment]) -> Option<&StoreNode> {
        let mut current = &self.data;

        for elem in path {
            current = current.get_child(elem)?;
        }

        Some(current)
    }

    fn get_or_create_lock_node(&mut self, path: Box<[RegularKeySegment]>) -> &mut LockNode {
        let mut current = &mut self.locks;

        for elem in path {
            current = current.get_or_create_child(elem).0;
        }

        current
    }

    pub fn delete(
        &mut self,
        path: &[RegularKeySegment],
    ) -> StoreResult<Option<(Value, Option<Vec<AffectedLsSubscribers>>)>> {
        let mut ls_subscribers = None;
        let removed = Store::ndelete(
            &mut self.data,
            path,
            Some(&self.subscribers),
            &mut ls_subscribers,
        )?;
        if removed.is_some() {
            self.len -= 1;
        }
        debug_assert!(self.data.is_empty() || self.data.is_clean());
        Ok(removed.map(|it| (it, ls_subscribers)))
    }

    pub fn delete_lock_node(&mut self, path: &[RegularKeySegment]) {
        Store::ndelete_lock_nodes(&mut self.locks, path);
        debug_assert!(self.locks.is_empty() || self.locks.is_clean());
    }

    fn ndelete_lock_nodes(node: &mut LockNode, relative_path: &[RegularKeySegment]) {
        if relative_path.is_empty() {
            node.take_value();
            return;
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        if let Some(next) = node.get_child_mut(head) {
            Store::ndelete_lock_nodes(next, tail);
            node.trim();
        }
    }

    /// retrieve values for a key containing at least one single-level wildcard and possibly a multi-level wildcard
    pub fn get_matches(&self, path: &[KeySegment]) -> StoreResult<Vec<KeyValuePair>> {
        let mut matches = Vec::new();
        let traversed = vec![];
        Store::ncollect_matches(&self.data, traversed, path, &mut matches, None, &mut None)?;
        Ok(matches)
    }

    pub fn delete_matches(
        &mut self,
        path: &[KeySegment],
    ) -> StoreResult<(Vec<KeyValuePair>, Option<Vec<AffectedLsSubscribers>>)> {
        let mut ls_subscribers = None;
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
        debug_assert!(self.data.is_empty() || self.data.is_clean());
        Ok((matches, ls_subscribers))
    }

    fn ndelete(
        node: &mut StoreNode,
        relative_path: &[RegularKeySegment],
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Option<Vec<(Vec<LsSubscriber>, Vec<String>)>>,
    ) -> StoreResult<Option<Value>> {
        if relative_path.is_empty() {
            return Ok(node.take_value().map(Value::from));
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        if let Some(next) = node.get_child_mut(head) {
            let val = Store::ndelete(
                next,
                tail,
                subscribers.and_then(|s| s.tree.get(head)),
                ls_subscribers,
            )?;
            if node.trim() {
                let new_children = node.ls_owned();
                if let Some(subscribers) = subscribers.as_ref()
                    && !subscribers.ls_subscribers.is_empty()
                {
                    let subscribers = subscribers.ls_subscribers.clone();
                    ls_subscribers
                        .get_or_insert_default()
                        .push((subscribers, new_children));
                }
            }
            Ok(val)
        } else {
            Ok(None)
        }
    }

    fn ndelete_matches(
        node: &mut StoreNode,
        traversed_path: Vec<&str>,
        matches: &mut KeyValuePairs,
        relative_path: &[KeySegment],
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Option<Vec<(Vec<LsSubscriber>, Vec<String>)>>,
    ) -> StoreResult<()> {
        if relative_path.is_empty() {
            if let Some(value) = node.take_value() {
                let key = traversed_path.join("/");
                matches.push(KeyValuePair::new(key, value.into()));
            }
            return Ok(());
        }

        let head = &relative_path[0];
        let tail = &relative_path[1..];

        match &head {
            KeySegment::MultiWildcard => {
                if !tail.is_empty() {
                    return Err(StoreError::IllegalMultiWildcard(format!(
                        "{}/{}/{}",
                        &format_path(&traversed_path),
                        head.as_ref(),
                        &format_path(tail),
                    )));
                }
                Store::ncollect_matches(
                    node,
                    traversed_path.clone(),
                    &[KeySegment::MultiWildcard],
                    matches,
                    subscribers,
                    ls_subscribers,
                )?;
                node.drop_children();
            }
            KeySegment::Wildcard => {
                for id in node.ls_owned() {
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
        node.trim();

        Ok(())
    }

    fn ndelete_child_matches<'a>(
        node: &mut StoreNode,
        id: &'a RegularKeySegment,
        mut traversed_path: Vec<&'a str>,
        matches: &mut KeyValuePairs,
        relative_path: &[KeySegment],
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Option<Vec<(Vec<LsSubscriber>, Vec<String>)>>,
    ) -> StoreResult<()> {
        traversed_path.push(id);

        if let Some(child) = node.get_child_mut(id) {
            Store::ndelete_matches(
                child,
                traversed_path,
                matches,
                relative_path,
                subscribers.and_then(|s| s.tree.get(id)),
                ls_subscribers,
            )?;
            if node.trim() {
                let new_children = node.ls_owned();
                if let Some(subscribers) = subscribers.as_ref()
                    && !subscribers.ls_subscribers.is_empty()
                {
                    let subscribers = subscribers.ls_subscribers.clone();
                    ls_subscribers
                        .get_or_insert_default()
                        .push((subscribers, new_children));
                }
            }
        }

        Ok(())
    }

    fn ncollect_matches<'p>(
        node: &StoreNode,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &'p [KeySegment],
        matches: &mut Vec<KeyValuePair>,
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Option<Vec<(Vec<LsSubscriber>, Vec<String>)>>,
    ) -> StoreResult<()> {
        if remaining_path.is_empty() {
            if let Some(value) = node.value().map(AsRef::as_ref) {
                let key = traversed_path.join("/");
                matches.push(KeyValuePair::new(key, value.to_owned()));
            }

            return Ok(());
        }

        let next = &remaining_path[0];
        let tail = &remaining_path[1..];

        match next {
            KeySegment::MultiWildcard => {
                if !tail.is_empty() {
                    return Err(StoreError::IllegalMultiWildcard(format!(
                        "{}/{}/{}",
                        &format_path(&traversed_path),
                        next.as_ref(),
                        &format_path(tail),
                    )));
                }

                if let Some(value) = node.value().map(AsRef::as_ref) {
                    let key = traversed_path.join("/");
                    matches.push(KeyValuePair::new(key, value.to_owned()));
                }

                if let Some(tree) = node.sub_tree() {
                    for (key, node) in tree {
                        let new_children = vec![];
                        if let Some(subscribers) = subscribers.as_ref()
                            && !subscribers.ls_subscribers.is_empty()
                        {
                            let subscribers = subscribers.ls_subscribers.clone();
                            ls_subscribers
                                .get_or_insert_default()
                                .push((subscribers, new_children));
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
            }
            KeySegment::Wildcard => {
                if let Some(tree) = node.sub_tree() {
                    for (key, node) in tree {
                        let new_children = vec![];
                        if let Some(subscribers) = subscribers.as_ref()
                            && !subscribers.ls_subscribers.is_empty()
                        {
                            let subscribers = subscribers.ls_subscribers.clone();
                            ls_subscribers
                                .get_or_insert_default()
                                .push((subscribers, new_children));
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
            }
            KeySegment::Regular(elem) => {
                traversed_path.push(elem);
                if let Some(child) = node.get_child(elem) {
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
        node: &StoreNode,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &'p [KeySegment],
        children: &mut HashSet<RegularKeySegment>,
        subscribers: Option<&SubscribersNode>,
        ls_subscribers: &mut Vec<(Vec<LsSubscriber>, Vec<String>)>,
    ) -> StoreResult<()> {
        if remaining_path.is_empty() {
            children.extend(node.ls_owned());
            return Ok(());
        }

        let next = &remaining_path[0];
        let tail = &remaining_path[1..];

        match next {
            KeySegment::MultiWildcard => {
                return Err(StoreError::IllegalMultiWildcard(format!(
                    "{}/{}/{}",
                    &format_path(&traversed_path),
                    next.as_ref(),
                    &format_path(tail),
                )));
            }
            KeySegment::Wildcard => {
                if let Some(tree) = node.sub_tree() {
                    for (key, node) in tree {
                        let new_children = vec![];
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
            }
            KeySegment::Regular(elem) => {
                traversed_path.push(elem);
                if let Some(child) = node.get_child(elem) {
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
    ) -> StoreResult<(bool, Option<Vec<AffectedLsSubscribers>>)> {
        self.insert(path, ValueEntry::Plain(value))
    }

    pub fn insert_cas(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
        version: u64,
    ) -> StoreResult<(bool, Option<Vec<AffectedLsSubscribers>>)> {
        self.insert(path, ValueEntry::Cas(value, version))
    }

    fn insert(
        &mut self,
        path: &[RegularKeySegment],
        value: ValueEntry,
    ) -> StoreResult<(bool, Option<Vec<AffectedLsSubscribers>>)> {
        let mut ls_subscribers: Option<Vec<(Vec<LsSubscriber>, &[String])>> = None;
        let mut current_node = &mut self.data;
        let mut current_subscribers = Some(&self.subscribers);

        for (i, elem) in path.iter().enumerate() {
            let (child, created) = current_node.get_or_create_child(elem.to_owned());
            current_node = child;
            if created
                && let Some(subscribers) = current_subscribers
                && !subscribers.ls_subscribers.is_empty()
            {
                let subscribers = subscribers.ls_subscribers.clone();
                ls_subscribers
                    .get_or_insert_default()
                    .push((subscribers, &path[0..i]));
            }

            current_subscribers = current_subscribers.and_then(|node| node.tree.get(elem));
        }

        let (value_existed, value_changed, value) = match (current_node.value(), value) {
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

        current_node.set_value(value);

        if !value_existed {
            self.len += 1;
        }

        let ls_subscribers = ls_subscribers.map(|it| {
            it.into_iter()
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
                .collect()
        });

        Ok((value_changed, ls_subscribers))
    }

    pub fn ls(&self, path: &[impl AsRef<str>]) -> Option<Vec<RegularKeySegment>> {
        if path.is_empty() {
            panic!("path must not be empty!");
        }
        let mut current = &self.data;

        for elem in path {
            current = current.get_child(elem.as_ref())?;
        }

        Some(current.ls_owned())
    }

    pub fn ls_root(&self) -> Vec<RegularKeySegment> {
        self.data.ls_owned()
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

    pub fn merge(&mut self, other: StoreNode) -> Vec<(String, (ValueEntry, bool))> {
        let mut insertions = Vec::new();
        let path = Vec::new();
        Store::nmerge(&mut self.data, other, None, &mut insertions, &path);
        self.len = Store::ncount_values(&self.data);
        insertions
    }

    pub fn count_entries(&mut self) {
        self.len = Store::ncount_values(&self.data);
    }

    fn nmerge(
        node: &mut StoreNode,
        mut other: StoreNode,
        key: Option<&str>,
        insertions: &mut Vec<(String, (ValueEntry, bool))>,
        path: &[&str],
    ) {
        if let Some(v) = other.take_value() {
            let changed = node.value() != Some(&v);
            node.set_value(v.clone());
            let key = concat_key(path, key);
            debug!("Imported {} = {:?}", key, v);
            insertions.push((key, (v, changed)));
        }

        let mut path = path.to_owned();
        if let Some(key) = key {
            path.push(key);
        }

        if let Some(tree) = other.into_sub_tree() {
            for (key, other_node) in tree {
                let own_node = node.get_or_create_child(key.to_owned()).0;
                Store::nmerge(own_node, other_node, Some(&key), insertions, &path);
            }
        }
    }

    fn ncount_values(node: &StoreNode) -> usize {
        let mut count = if node.value().is_some() { 1 } else { 0 };
        if let Some(tree) = node.sub_tree() {
            for child in tree.values() {
                count += Store::ncount_values(child);
            }
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
        match &mut node.value() {
            Some(lock) => {
                if client_id == lock.holder {
                    debug!("Client {client_id} already holds the lock on {path:?}");
                } else {
                    return Err(WorterbuchError::KeyIsLocked(path.join("/")));
                }
            }
            None => {
                node.set_value(Lock::new(client_id));
                let paths = self.locked_keys.entry(client_id).or_default();
                paths.push(path);
            }
        }

        Ok(())
    }

    pub async fn acquire_lock(
        &mut self,
        client_id: Uuid,
        path: Box<[RegularKeySegment]>,
    ) -> (oneshot::Receiver<()>, Option<Uuid>) {
        let (tx, rx) = oneshot::channel();
        let node = self.get_or_create_lock_node(path.clone());
        let holder = match &mut node.value_mut() {
            Some(lock) => {
                if client_id == lock.holder {
                    debug!("Client {client_id} already holds the lock on {path:?}");
                    tx.send(()).ok();
                    Some(client_id)
                } else {
                    lock.queue(client_id, tx).await;
                    Some(lock.holder)
                }
            }
            None => {
                node.set_value(Lock::new(client_id));
                tx.send(()).ok();
                Some(client_id)
            }
        };

        let paths = self.locked_keys.entry(client_id).or_default();
        paths.push(path);

        (rx, holder)
    }

    pub async fn unlock_all(&mut self, client_id: Uuid) -> Option<Vec<(String, Option<Uuid>)>> {
        if let Some(paths) = self.locked_keys.remove(&client_id) {
            let mut out = vec![];
            for path in paths {
                let client_id = self.unlock(client_id, &path).await.ok().flatten();
                out.push((path.join("/"), client_id));
            }
            Some(out)
        } else {
            None
        }
    }

    pub async fn unlock(
        &mut self,
        client_id: Uuid,
        path: &[RegularKeySegment],
    ) -> WorterbuchResult<Option<Uuid>> {
        let node = self.get_or_create_lock_node(path.into());

        if let Some(lock) = node.value_mut() {
            let (was_holder, new_holder) = lock.release(client_id).await;
            if !was_holder {
                return Err(WorterbuchError::KeyIsLocked(path.join("/")));
            } else if new_holder.is_none() {
                self.delete_lock_node(path);
            }
            Ok(new_holder)
        } else {
            warn!("Node {path:?} is not locked.");
            Err(WorterbuchError::KeyIsNotLocked(path.join("/")))
        }
    }

    pub(crate) fn reset(&mut self, data: StoreNode) {
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
        assert!(subscribers.is_none());

        let (tx, _) = mpsc::channel(1);
        let subscriber = LsSubscriber::new(
            SubscriptionId::new(Uuid::new_v4(), 123),
            parent.clone().into(),
            tx,
        );
        store.add_ls_subscriber(&parent, subscriber);
        let (_, subscribers) = store.insert(&path, json!("Hello There!").into()).unwrap();
        assert!(subscribers.is_none());
    }

    #[test]
    fn delete_detects_ls_subscribers_not_empty() {
        let mut store = Store::default();
        let parent = ["hello".to_owned(), "there".to_owned()];
        let path = ["hello".to_owned(), "there".to_owned(), "world".to_owned()];
        let path2 = ["hello".to_owned(), "there".to_owned(), "you".to_owned()];

        let (_, subscribers) = store.insert(&path, json!("Hello!").into()).unwrap();
        assert!(subscribers.is_none());

        let (tx, _) = mpsc::channel(1);
        let subscriber = LsSubscriber::new(
            SubscriptionId::new(Uuid::new_v4(), 123),
            parent.clone().into(),
            tx,
        );
        store.add_ls_subscriber(&parent, subscriber);
        store.insert(&path, json!("Hello There!").into()).unwrap();
        let subscribers = store.delete(&path).unwrap().unwrap().1.unwrap();
        assert_eq!(subscribers.len(), 1);

        store.insert(&path, json!("Hello There!").into()).unwrap();
        store.insert(&path2, json!("Hello There!").into()).unwrap();
        let subscribers = store.delete(&path).unwrap().unwrap().1.unwrap();
        assert_eq!(subscribers.len(), 1);
    }

    #[test]
    fn pdelete_detects_ls_subscribers_not_empty() {
        let mut store = Store::default();
        let parent = ["hello".to_owned(), "there".to_owned()];
        let path = ["hello".to_owned(), "there".to_owned(), "world".to_owned()];
        let del_path = ["hello".into(), "there".into(), KeySegment::Wildcard];

        let (_, subscribers) = store.insert(&path, json!("Hello!").into()).unwrap();
        assert!(subscribers.is_none());

        let (tx, _) = mpsc::channel(1);
        let subscriber = LsSubscriber::new(
            SubscriptionId::new(Uuid::new_v4(), 123),
            parent.clone().into(),
            tx,
        );
        store.add_ls_subscriber(&parent, subscriber);
        store.insert(&path, json!("Hello There!").into()).unwrap();
        let subscribers = store.delete_matches(&del_path).unwrap().1.unwrap();
        assert_eq!(subscribers.len(), 1);
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
        let subscribers = subscribers.unwrap();
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
        assert_eq!(store.cget(&path), Some((&json!(0), 1)));
        assert!(store.insert_cas(&path, json!(1), 1).is_ok());
        assert_eq!(store.cget(&path), Some((&json!(1), 2)));
    }

    #[test]
    fn regular_set_cannot_overwrite_cas_value() {
        let mut store = Store::default();
        let path = reg_key_segs("hello/cas");
        assert!(store.insert_cas(&path, json!(0), 0).is_ok());
        assert_eq!(store.cget(&path), Some((&json!(0), 1)));
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
        store.unlock(client_1, &path).await.unwrap();
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
        store.unlock(client_1, &path).await.unwrap();
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
        store.unlock(client_1, &path).await.unwrap();
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
