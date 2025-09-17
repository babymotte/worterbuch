use crate::{
    store::error::StoreResult,
    subscribers::{self, LsSubs, Subs},
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
};
use worterbuch_common::{
    CasVersion, KeySegment, KeyValuePair, KeyValuePairs, RegularKeySegment, Value,
};

pub mod backend;
pub mod error;
pub mod lock;

type Tree<V> = HashMap<RegularKeySegment, Node<V>>;
pub type AffectedLsSubscribers = (LsSubs, Vec<RegularKeySegment>);

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

pub struct InsertionResult {
    value_changed: bool,
    subs: Subs,
    ls_subs: Vec<AffectedLsSubscribers>,
}

pub struct DeletionResult {
    deleted: KeyValuePairs,
    subs: Subs,
    ls_subs: Vec<AffectedLsSubscribers>,
}

pub trait Store {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn insert_plain(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
        subs: &subscribers::Node,
    ) -> StoreResult<InsertionResult>;

    fn insert_cas(
        &mut self,
        path: &[RegularKeySegment],
        value: Value,
        version: u64,
        subs: &subscribers::Node,
    ) -> StoreResult<InsertionResult>;

    fn get(&self, path: &[RegularKeySegment]) -> Option<Cow<'_, Value>>;

    fn cget(&self, path: &[RegularKeySegment]) -> Option<(Cow<'_, Value>, CasVersion)>;

    fn pget(&self, path: &[KeySegment]) -> StoreResult<Vec<KeyValuePair>>;

    fn delete(&mut self, path: &[RegularKeySegment]) -> StoreResult<DeletionResult>;

    fn pdelete(&mut self, path: &[KeySegment]) -> StoreResult<DeletionResult>;

    fn ls(&self, path: Option<&[impl AsRef<str>]>) -> StoreResult<BTreeSet<RegularKeySegment>>;

    fn pls(&self, path: &[KeySegment]) -> StoreResult<BTreeSet<RegularKeySegment>>;

    fn export(&mut self) -> Node<ValueEntry>;

    fn import(
        &mut self,
        data: Node<ValueEntry>,
        subs: &subscribers::Node,
    ) -> Vec<(KeyValuePair, InsertionResult)>;

    fn reset(&mut self, data: Node<ValueEntry>);
}
