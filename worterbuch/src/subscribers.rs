use anyhow::Result;
use std::collections::{hash_map::Entry, HashMap};
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;
use worterbuch_common::{KeySegment, PStateEvent, RegularKeySegment, TransactionId};

type Tree = HashMap<KeySegment, Node>;

#[derive(Debug, Clone, Default)]
pub struct Subs {
    pub subscribers: Vec<Subscriber>,
    pub ls_subscribers: Vec<LsSubscriber>,
}

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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct LsSubscriber {
    parent: Vec<KeySegment>,
    tx: UnboundedSender<Vec<RegularKeySegment>>,
    id: SubscriptionId,
}

impl LsSubscriber {
    pub fn new(
        id: SubscriptionId,
        parent: Vec<KeySegment>,
        tx: UnboundedSender<Vec<RegularKeySegment>>,
    ) -> LsSubscriber {
        LsSubscriber { parent, tx, id }
    }

    pub fn send(&self, children: Vec<RegularKeySegment>) -> Result<()> {
        self.tx.send(children)?;
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

        self.add_matches(&self.data, key, &mut all_subscribers);

        all_subscribers
    }

    pub fn get_ls_subscribers(&self, key: &[RegularKeySegment]) -> Vec<LsSubscriber> {
        let mut all_subscribers = Vec::new();

        self.add_ls_matches(&self.data, key, &mut all_subscribers);

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
        all_subscribers.extend(current.subscribers.subscribers.clone());
    }

    fn add_ls_matches(
        &self,
        mut current: &Node,
        remaining_path: &[RegularKeySegment],
        all_subscribers: &mut Vec<LsSubscriber>,
    ) {
        let mut remaining_path = remaining_path;

        for elem in remaining_path {
            remaining_path = &remaining_path[1..];

            if let Some(node) = current.tree.get(&KeySegment::Wildcard) {
                self.add_ls_matches(&node, remaining_path, all_subscribers);
            }

            if let Some(node) = current.tree.get(&KeySegment::MultiWildcard) {
                self.add_all_ls_children(node, all_subscribers);
            }

            if let Some(node) = current.tree.get(&elem.to_owned().into()) {
                current = node;
            } else {
                return;
            }
        }
        all_subscribers.extend(current.subscribers.ls_subscribers.clone());
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

        current.subscribers.subscribers.push(subscriber);
    }

    pub fn add_ls_subscriber(&mut self, parent: &[KeySegment], subscriber: LsSubscriber) {
        log::debug!("Adding ls subscriber for parent {:?}", parent);
        let mut current = &mut self.data;

        for elem in parent {
            current = match current.tree.entry(elem.to_owned().into()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(Node::default()),
            };
        }

        current.subscribers.ls_subscribers.push(subscriber);
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
        current.subscribers.subscribers.retain(|s| {
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

    pub fn unsubscribe_ls(&mut self, parent: &[KeySegment], subscription: &SubscriptionId) -> bool {
        let mut current = &mut self.data;

        for elem in parent {
            if let Some(node) = current.tree.get_mut(elem) {
                current = node;
            } else {
                log::warn!("No subscriber found for pattern {:?}", parent);
                return false;
            }
        }
        let mut removed = false;
        current.subscribers.ls_subscribers.retain(|s| {
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

        current
            .subscribers
            .subscribers
            .retain(|s| s.id != subscriber.id);
    }

    pub fn remove_ls_subscriber(&mut self, subscriber: LsSubscriber) {
        let mut current = &mut self.data;

        for elem in &subscriber.parent {
            if let Some(node) = current.tree.get_mut(elem) {
                current = node;
            } else {
                log::warn!("No ls subscriber found for parent {:?}", subscriber.parent);
                return;
            }
        }

        current
            .subscribers
            .ls_subscribers
            .retain(|s| s.id != subscriber.id);
    }

    fn add_all_children(&self, node: &Node, all_subscribers: &mut Vec<Subscriber>) {
        all_subscribers.extend(node.subscribers.subscribers.clone());
        for node in node.tree.values() {
            self.add_all_children(&node, all_subscribers);
        }
    }

    fn add_all_ls_children(&self, node: &Node, all_subscribers: &mut Vec<LsSubscriber>) {
        all_subscribers.extend(node.subscribers.ls_subscribers.clone());
        for node in node.tree.values() {
            self.add_all_ls_children(&node, all_subscribers);
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
