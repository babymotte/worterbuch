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

use miette::{IntoDiagnostic, Result, miette};
use std::collections::{HashMap, hash_map::Entry};
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};
use worterbuch_common::{KeySegment, PStateEvent, RegularKeySegment, StateEvent, SubscriptionId};

type Subs = Vec<Subscriber>;
type Tree = HashMap<KeySegment, Node>;

#[derive(Clone, Debug)]
pub enum EventSender {
    State(Sender<StateEvent>),
    PState(Sender<PStateEvent>),
}

#[derive(Clone, Debug)]
pub struct Subscriber {
    pattern: Vec<KeySegment>,
    tx: EventSender,
    id: SubscriptionId,
    unique: bool,
}

impl Subscriber {
    pub fn new(
        id: SubscriptionId,
        pattern: Vec<KeySegment>,
        tx: EventSender,
        unique: bool,
    ) -> Subscriber {
        Subscriber {
            pattern,
            tx,
            id,
            unique,
        }
    }

    pub async fn send_pstate(&self, event: PStateEvent) -> Result<()> {
        if let EventSender::PState(tx) = &self.tx {
            tx.send(event).await.into_diagnostic()?;
            Ok(())
        } else {
            Err(miette!(
                "Tried to send a PSatetEvent to a StateEvent subscriber"
            ))
        }
    }

    pub async fn send_state(&self, event: StateEvent) -> Result<()> {
        if let EventSender::State(tx) = &self.tx {
            tx.send(event).await.into_diagnostic()?;
            Ok(())
        } else {
            Err(miette!(
                "Tried to send a SatetEvent to a PStateEvent subscriber"
            ))
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
    tx: Sender<Vec<RegularKeySegment>>,
    pub id: SubscriptionId,
}

impl LsSubscriber {
    pub fn new(
        id: SubscriptionId,
        parent: Vec<RegularKeySegment>,
        tx: Sender<Vec<RegularKeySegment>>,
    ) -> LsSubscriber {
        LsSubscriber { parent, tx, id }
    }

    pub async fn send(&self, children: Vec<RegularKeySegment>) -> Result<()> {
        self.tx.send(children).await.into_diagnostic()?;
        Ok(())
    }
}

#[derive(Default)]
pub struct Node {
    pub subscribers: Subs,
    pub tree: Tree,
}

#[derive(Default)]
pub struct Subscribers {
    data: Node,
}

impl Subscribers {
    pub fn get_subscribers(&self, key: &[RegularKeySegment]) -> Vec<Subscriber> {
        let mut all_subscribers = Vec::new();

        add_matches(&self.data, key, &mut all_subscribers);

        all_subscribers
    }

    pub fn add_subscriber(&mut self, pattern: &[KeySegment], subscriber: Subscriber) {
        debug!("Adding subscriber for pattern {:?}", pattern);
        let mut current = &mut self.data;

        for elem in pattern {
            current = match current.tree.entry(elem.to_owned()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(Node::default()),
            };
        }

        current.subscribers.push(subscriber);
    }

    pub fn unsubscribe(&mut self, pattern: &[KeySegment], subscription: &SubscriptionId) -> bool {
        let mut current = &mut self.data;

        for elem in pattern {
            if let Some(node) = current.tree.get_mut(elem) {
                current = node;
            } else {
                warn!("No subscriber found for pattern {:?}", pattern);
                return false;
            }
        }
        let mut removed = false;
        current.subscribers.retain(|s| {
            let retain = &s.id != subscription;
            removed = removed || !retain;
            if !retain {
                debug!("Removing subscription {subscription:?} to pattern {pattern:?}");
            }
            retain
        });
        if !removed {
            debug!("no matching subscription found")
        }
        removed
    }

    pub fn remove_subscriber(&mut self, subscriber: Subscriber) {
        let mut current = &mut self.data;

        for elem in &subscriber.pattern {
            if let Some(node) = current.tree.get_mut(elem) {
                current = node;
            } else {
                warn!("No subscriber found for pattern {:?}", subscriber.pattern);
                return;
            }
        }

        current.subscribers.retain(|s| s.id != subscriber.id);
    }
}

fn add_matches(
    mut current: &Node,
    remaining_path: &[RegularKeySegment],
    all_subscribers: &mut Vec<Subscriber>,
) {
    let mut remaining_path = remaining_path;

    for elem in remaining_path {
        remaining_path = &remaining_path[1..];

        if let Some(node) = current.tree.get(&KeySegment::Wildcard) {
            add_matches(node, remaining_path, all_subscribers);
        }

        if let Some(node) = current.tree.get(&KeySegment::MultiWildcard) {
            add_all_children(node, all_subscribers);
        }

        if let Some(node) = current.tree.get(&elem.to_owned().into()) {
            current = node;
        } else {
            return;
        }
    }
    all_subscribers.extend(current.subscribers.clone());
}

fn add_all_children(node: &Node, all_subscribers: &mut Vec<Subscriber>) {
    all_subscribers.extend(node.subscribers.clone());
    for node in node.tree.values() {
        add_all_children(node, all_subscribers);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::sync::mpsc::channel;
    use uuid::Uuid;
    use worterbuch_common::parse_segments;

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
            client_id: Uuid::new_v4(),
            transaction_id: 123,
        };
        let subscriber = Subscriber::new(
            id,
            pattern.clone().into_iter().map(|s| s.to_owned()).collect(),
            EventSender::PState(tx),
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
            client_id: Uuid::new_v4(),
            transaction_id: 123,
        };
        let subscriber = Subscriber::new(
            id.clone(),
            pattern.clone().into_iter().map(|s| s.to_owned()).collect(),
            EventSender::PState(tx),
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
}
