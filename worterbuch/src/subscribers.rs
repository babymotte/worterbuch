use anyhow::Result;
use std::collections::{hash_map::Entry, HashMap};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use worterbuch_common::{KeySegment, PStateEvent, RegularKeySegment, TransactionId};

type Subs = Vec<Subscriber>;
type Tree = HashMap<KeySegment, Node>;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SubscriptionId {
    pub client_id: Uuid,
    pub transaction_id: TransactionId,
}

impl SubscriptionId {
    pub fn new(client_id: Uuid, transaction_id: TransactionId) -> Self {
        SubscriptionId {
            client_id,
            transaction_id,
        }
    }
}

#[derive(Clone)]
pub struct Subscriber {
    pattern: Vec<KeySegment>,
    tx: UnboundedSender<PStateEvent>,
    id: SubscriptionId,
    unique: bool,
}

impl Subscriber {
    pub fn new(
        id: SubscriptionId,
        pattern: Vec<KeySegment>,
        tx: UnboundedSender<PStateEvent>,
        unique: bool,
    ) -> Subscriber {
        Subscriber {
            pattern,
            tx,
            id,
            unique,
        }
    }

    pub fn send(&self, event: PStateEvent) -> Result<()> {
        self.tx.send(event)?;
        Ok(())
    }

    pub fn is_unique(&self) -> bool {
        self.unique
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

        self.add_matches(&self.data, key, &mut all_subscribers);

        all_subscribers
    }

    fn add_matches(
        &self,
        mut current: &Node,
        remaining_path: &[RegularKeySegment],
        all_subscribers: &mut Vec<Subscriber>,
    ) {
        let mut remaining_path = remaining_path;

        for elem in remaining_path {
            remaining_path = &remaining_path[1..];

            if let Some(node) = current.tree.get(&KeySegment::Wildcard) {
                self.add_matches(&node, remaining_path, all_subscribers);
            }

            if let Some(node) = current.tree.get(&KeySegment::MultiWildcard) {
                self.add_all_children(node, all_subscribers);
            }

            if let Some(node) = current.tree.get(&elem.to_owned().into()) {
                current = node;
            } else {
                return;
            }
        }
        all_subscribers.extend(current.subscribers.clone());
    }

    pub fn add_subscriber(&mut self, pattern: &[KeySegment], subscriber: Subscriber) {
        log::debug!("Adding subscriber for pattern {:?}", pattern);
        let mut current = &mut self.data;

        for elem in pattern {
            current = match current.tree.entry(elem.to_owned().into()) {
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
                log::warn!("No subscriber found for pattern {:?}", pattern);
                return false;
            }
        }
        let mut removed = false;
        current.subscribers.retain(|s| {
            let retain = &s.id != subscription;
            removed = removed || !retain;
            if !retain {
                log::debug!("Removing subscription {subscription:?} to pattern {pattern:?}");
            }
            retain
        });
        if !removed {
            log::debug!("no matching subscription found")
        }
        removed
    }

    pub fn remove_subscriber(&mut self, subscriber: Subscriber) {
        let mut current = &mut self.data;

        for elem in &subscriber.pattern {
            if let Some(node) = current.tree.get_mut(elem) {
                current = node;
            } else {
                log::warn!("No subscriber found for pattern {:?}", subscriber.pattern);
                return;
            }
        }

        current.subscribers.retain(|s| s.id != subscriber.id);
    }

    fn add_all_children(&self, node: &Node, all_subscribers: &mut Vec<Subscriber>) {
        all_subscribers.extend(node.subscribers.clone());
        for node in node.tree.values() {
            self.add_all_children(&node, all_subscribers);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;
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

        let (tx, _rx) = unbounded_channel();
        let pattern = KeySegment::parse("test/?/b/#");
        let id = SubscriptionId {
            client_id: Uuid::new_v4(),
            transaction_id: 123,
        };
        let subscriber = Subscriber::new(
            id,
            pattern.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx,
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

        let (tx, _rx) = unbounded_channel();
        let pattern = key_segs("test/?/b/#");
        let id = SubscriptionId {
            client_id: Uuid::new_v4(),
            transaction_id: 123,
        };
        let subscriber = Subscriber::new(
            id.clone(),
            pattern.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx,
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
