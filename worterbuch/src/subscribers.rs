/*
 *  Worterbuch subscriber management module
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

use hashbrown::HashMap;
use miette::{IntoDiagnostic, Result, bail};
use tracing::debug;
use worterbuch_common::{
    KeySegment, LsSubscriptionSender, PSubscriptionSender, RegularKeySegment, SubscriptionId,
    SubscriptionSender,
    protocol::v1::{PStateEvent, StateEvent, Trace},
};

type Subs = Vec<Subscriber>;
type Tree = HashMap<RegularKeySegment, Node>;

#[derive(Clone, Debug)]
pub enum EventSender {
    State(SubscriptionSender),
    PState(PSubscriptionSender),
}

#[derive(Clone, Debug)]
pub struct Subscriber {
    pattern: Vec<KeySegment>,
    tx: EventSender,
    id: SubscriptionId,
    unique: bool,
    send_traces: bool,
}

impl Subscriber {
    pub fn new(
        id: SubscriptionId,
        pattern: Vec<KeySegment>,
        tx: EventSender,
        unique: bool,
        send_traces: bool,
    ) -> Subscriber {
        Subscriber {
            pattern,
            tx,
            id,
            unique,
            send_traces,
        }
    }

    pub async fn send_pstate(&self, event: PStateEvent, trace: Trace) -> Result<()> {
        if let EventSender::PState(tx) = &self.tx {
            let trace = if self.send_traces { Some(trace) } else { None };
            tx.send((event, trace)).await.into_diagnostic()?;
            Ok(())
        } else {
            bail!("Tried to send a PStateEvent to a StateEvent subscriber");
        }
    }

    pub async fn send_state(&self, event: StateEvent, trace: Trace) -> Result<()> {
        if let EventSender::State(tx) = &self.tx {
            let trace = if self.send_traces { Some(trace) } else { None };
            tx.send((event, trace)).await.into_diagnostic()?;
            Ok(())
        } else {
            bail!("Tried to send a StateEvent to a PStateEvent subscriber");
        }
    }

    pub fn is_unique(&self) -> bool {
        self.unique
    }

    pub fn is_pstate_subscriber(&self) -> bool {
        match self.tx {
            EventSender::State(_) => false,
            EventSender::PState(_) => true,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LsSubscriber {
    pub parent: Vec<RegularKeySegment>,
    tx: LsSubscriptionSender,
    pub id: SubscriptionId,
    send_traces: bool,
}

impl LsSubscriber {
    pub fn new(
        id: SubscriptionId,
        parent: Vec<RegularKeySegment>,
        tx: LsSubscriptionSender,
        send_traces: bool,
    ) -> LsSubscriber {
        LsSubscriber {
            parent,
            tx,
            id,
            send_traces,
        }
    }

    pub async fn send(&self, children: Vec<RegularKeySegment>, trace: Trace) -> Result<()> {
        let trace = if self.send_traces { Some(trace) } else { None };
        self.tx.send((children, trace)).await.into_diagnostic()?;
        Ok(())
    }
}

/// A node in the subscriber tree. Children for regular key segments are kept in a map keyed by
/// the plain segment string, so lookups for concrete keys need no allocation. Wildcard children
/// are kept separately, so they can be checked without a map lookup.
#[derive(Default)]
struct Node {
    subscribers: Subs,
    tree: Tree,
    wildcard: Option<Box<Node>>,
    multi_wildcard: Option<Box<Node>>,
}

impl Node {
    fn child_or_insert(&mut self, segment: &KeySegment) -> &mut Node {
        match segment {
            KeySegment::Regular(segment) => self.tree.entry_ref(segment.as_str()).or_default(),
            KeySegment::Wildcard => self.wildcard.get_or_insert_default(),
            KeySegment::MultiWildcard => self.multi_wildcard.get_or_insert_default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.subscribers.is_empty()
            && self.tree.is_empty()
            && self.wildcard.is_none()
            && self.multi_wildcard.is_none()
    }

    /// Removes the subscription registered for `pattern` (relative to this node) and prunes all
    /// nodes along the way that end up empty. Returns whether the subscription was found.
    fn remove(&mut self, pattern: &[KeySegment], subscription: &SubscriptionId) -> bool {
        let Some((head, tail)) = pattern.split_first() else {
            let before = self.subscribers.len();
            self.subscribers.retain(|s| &s.id != subscription);
            return self.subscribers.len() != before;
        };

        match head {
            KeySegment::Regular(segment) => {
                let Some(child) = self.tree.get_mut(segment.as_str()) else {
                    return false;
                };
                let removed = child.remove(tail, subscription);
                if child.is_empty() {
                    self.tree.remove(segment.as_str());
                }
                removed
            }
            KeySegment::Wildcard => remove_from_slot(&mut self.wildcard, tail, subscription),
            KeySegment::MultiWildcard => {
                remove_from_slot(&mut self.multi_wildcard, tail, subscription)
            }
        }
    }
}

fn remove_from_slot(
    slot: &mut Option<Box<Node>>,
    pattern: &[KeySegment],
    subscription: &SubscriptionId,
) -> bool {
    let Some(child) = slot else {
        return false;
    };
    let removed = child.remove(pattern, subscription);
    if child.is_empty() {
        *slot = None;
    }
    removed
}

#[derive(Default)]
pub struct Subscribers {
    data: Node,
}

impl Subscribers {
    pub fn get_subscribers(&self, key: &[RegularKeySegment]) -> Vec<&Subscriber> {
        let mut all_subscribers = Vec::new();

        add_matches(&self.data, key, &mut all_subscribers);

        all_subscribers
    }

    pub fn add_subscriber(&mut self, pattern: &[KeySegment], subscriber: Subscriber) {
        debug!("Adding subscriber for pattern {:?}", pattern);
        let mut current = &mut self.data;

        for elem in pattern {
            current = current.child_or_insert(elem);
        }

        current.subscribers.push(subscriber);
    }

    pub fn unsubscribe(&mut self, pattern: &[KeySegment], subscription: &SubscriptionId) -> bool {
        let removed = self.data.remove(pattern, subscription);
        if removed {
            debug!("Removed subscription {subscription:?} to pattern {pattern:?}");
        } else {
            debug!("No subscription {subscription:?} found for pattern {pattern:?}");
        }
        removed
    }

    pub fn remove_subscriber(&mut self, subscriber: &Subscriber) {
        self.unsubscribe(&subscriber.pattern, &subscriber.id);
    }
}

fn add_matches<'a>(
    mut current: &'a Node,
    remaining_path: &[RegularKeySegment],
    all_subscribers: &mut Vec<&'a Subscriber>,
) {
    let mut remaining_path = remaining_path;

    for elem in remaining_path {
        remaining_path = &remaining_path[1..];

        if let Some(node) = &current.wildcard {
            add_matches(node, remaining_path, all_subscribers);
        }

        if let Some(node) = &current.multi_wildcard {
            add_all_children(node, all_subscribers);
        }

        if let Some(node) = current.tree.get(elem.as_str()) {
            current = node;
        } else {
            return;
        }
    }
    all_subscribers.extend(&current.subscribers);
}

fn add_all_children<'a>(node: &'a Node, all_subscribers: &mut Vec<&'a Subscriber>) {
    all_subscribers.extend(&node.subscribers);
    for node in node.tree.values() {
        add_all_children(node, all_subscribers);
    }
    if let Some(node) = &node.wildcard {
        add_all_children(node, all_subscribers);
    }
    if let Some(node) = &node.multi_wildcard {
        add_all_children(node, all_subscribers);
    }
}

#[cfg(test)]
mod test {

    #![allow(clippy::as_conversions)]
    #![allow(clippy::unwrap_used)]

    use super::*;
    use tokio::sync::mpsc::channel;
    use worterbuch_common::{ClientId, parse_segments};

    fn reg_key_segs(key: &str) -> Vec<RegularKeySegment> {
        parse_segments(key).unwrap()
    }

    fn key_segs(key: &str) -> Vec<KeySegment> {
        KeySegment::parse(key)
    }

    #[test]
    fn get_subscribers() {
        let mut subscribers = Subscribers::default();

        let (tx, _rx) = channel(1);
        let pattern = KeySegment::parse("test/?/b/#");
        let id = SubscriptionId {
            client_id: ClientId::new_v4(),
            transaction_id: 123,
        };
        let subscriber = Subscriber::new(
            id,
            pattern.clone().into_iter().map(|s| s.to_owned()).collect(),
            EventSender::PState(tx),
            false,
            false,
        );

        subscribers.add_subscriber(&pattern, subscriber);

        let res = subscribers.get_subscribers(&reg_key_segs("test/a/b/c/d"));
        assert_eq!(res.len(), 1);

        let res = subscribers.get_subscribers(&reg_key_segs("test/a/b"));
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn subscribers_are_cleaned_up() {
        let mut subscribers = Subscribers::default();

        let (tx, _rx) = channel(1);
        let pattern = key_segs("test/?/b/#");
        let id = SubscriptionId {
            client_id: ClientId::new_v4(),
            transaction_id: 123,
        };
        let subscriber = Subscriber::new(
            id.clone(),
            pattern.clone().into_iter().map(|s| s.to_owned()).collect(),
            EventSender::PState(tx),
            false,
            false,
        );

        let res = subscribers.get_subscribers(&reg_key_segs("test/a/b/c/d"));
        assert_eq!(res.len(), 0);

        subscribers.add_subscriber(&pattern, subscriber);

        let res = subscribers.get_subscribers(&reg_key_segs("test/a/b/c/d"));
        assert_eq!(res.len(), 1);

        subscribers.unsubscribe(&pattern, &id);

        let res = subscribers.get_subscribers(&reg_key_segs("test/a/b/c/d"));
        assert_eq!(res.len(), 0);
    }

    fn add(subscribers: &mut Subscribers, pattern: &str, transaction_id: u64) -> Subscriber {
        let (tx, _rx) = channel(1);
        let pattern = key_segs(pattern);
        let id = SubscriptionId {
            client_id: ClientId::nil(),
            transaction_id,
        };
        let subscriber =
            Subscriber::new(id, pattern.clone(), EventSender::PState(tx), false, false);
        subscribers.add_subscriber(&pattern, subscriber.clone());
        subscriber
    }

    fn matches(subscribers: &Subscribers, key: &str) -> Vec<u64> {
        let mut tids: Vec<u64> = subscribers
            .get_subscribers(&reg_key_segs(key))
            .into_iter()
            .map(|s| s.id.transaction_id)
            .collect();
        tids.sort();
        tids
    }

    #[test]
    fn pattern_matching() {
        let mut subscribers = Subscribers::default();
        add(&mut subscribers, "a/b/c", 1);
        add(&mut subscribers, "a/?/c", 2);
        add(&mut subscribers, "a/#", 3);
        add(&mut subscribers, "?/b/#", 4);
        add(&mut subscribers, "#", 5);
        add(&mut subscribers, "a/b", 6);
        add(&mut subscribers, "a/b/c", 7);

        assert_eq!(matches(&subscribers, "a/b/c"), vec![1, 2, 3, 4, 5, 7]);
        assert_eq!(matches(&subscribers, "a/x/c"), vec![2, 3, 5]);
        // multi-wildcards do not match their parent
        assert_eq!(matches(&subscribers, "a/b"), vec![3, 5, 6]);
        assert_eq!(matches(&subscribers, "a"), vec![5]);
        assert_eq!(matches(&subscribers, "x/b/y/z"), vec![4, 5]);
        assert_eq!(matches(&subscribers, "x"), vec![5]);
    }

    #[test]
    fn unsubscribe_and_remove() {
        let mut subscribers = Subscribers::default();
        let s1 = add(&mut subscribers, "a/b/c", 1);
        let s2 = add(&mut subscribers, "a/?/c", 2);
        let s3 = add(&mut subscribers, "a/#", 3);

        assert!(subscribers.unsubscribe(&key_segs("a/?/c"), &s2.id));
        assert!(!subscribers.unsubscribe(&key_segs("a/?/c"), &s2.id));
        assert_eq!(matches(&subscribers, "a/b/c"), vec![1, 3]);

        subscribers.remove_subscriber(&s3);
        assert_eq!(matches(&subscribers, "a/b/c"), vec![1]);

        assert!(!subscribers.unsubscribe(&key_segs("x/y"), &s1.id));
        assert!(subscribers.unsubscribe(&key_segs("a/b/c"), &s1.id));
        assert!(matches(&subscribers, "a/b/c").is_empty());
    }

    #[test]
    fn empty_nodes_are_pruned() {
        let mut subscribers = Subscribers::default();
        let s1 = add(&mut subscribers, "a/b/c", 1);
        let s2 = add(&mut subscribers, "a/b/d", 2);
        let s3 = add(&mut subscribers, "a/?/#", 3);
        let s4 = add(&mut subscribers, "a/b", 4);

        assert!(subscribers.unsubscribe(&key_segs("a/b/c"), &s1.id));
        // siblings and their shared parents are kept
        let a = &subscribers.data.tree["a"];
        assert!(!a.tree["b"].tree.contains_key("c"));
        assert!(a.tree["b"].tree.contains_key("d"));
        assert_eq!(matches(&subscribers, "a/b/d"), vec![2, 3]);

        assert!(subscribers.unsubscribe(&key_segs("a/?/#"), &s3.id));
        assert!(subscribers.data.tree["a"].wildcard.is_none());

        assert!(subscribers.unsubscribe(&key_segs("a/b/d"), &s2.id));
        // "a/b" still has a subscriber of its own
        assert!(subscribers.data.tree["a"].tree["b"].tree.is_empty());

        subscribers.remove_subscriber(&s4);
        assert!(subscribers.data.is_empty());
    }
}
