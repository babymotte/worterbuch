use anyhow::Result;
use libworterbuch::codec::KeyValuePairs;
use std::collections::HashMap;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

type Subs = Vec<Subscriber>;
type Tree = HashMap<String, Node>;

#[derive(Clone)]
pub struct Subscriber {
    pattern: Vec<String>,
    tx: UnboundedSender<KeyValuePairs>,
    id: Uuid,
}

impl Subscriber {
    pub fn new(pattern: Vec<String>, tx: UnboundedSender<KeyValuePairs>) -> Subscriber {
        Subscriber {
            pattern,
            tx,
            id: Uuid::new_v4(),
        }
    }

    pub fn send(&self, event: KeyValuePairs) -> Result<()> {
        self.tx.send(event)?;
        Ok(())
    }

    pub fn id(&self) -> &Uuid {
        &self.id
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
    pub fn get_subscribers(
        &self,
        key: &[&str],
        wildcard: &str,
        multi_wildcard: &str,
    ) -> Vec<Subscriber> {
        let mut all_subscribers = Vec::new();

        self.add_matches(
            &self.data,
            key,
            wildcard,
            multi_wildcard,
            &mut all_subscribers,
        );

        all_subscribers
    }

    fn add_matches(
        &self,
        mut current: &Node,
        remaining_path: &[&str],
        wildcard: &str,
        multi_wildcard: &str,
        all_subscribers: &mut Vec<Subscriber>,
    ) {
        let mut remaining_path = remaining_path;

        for elem in remaining_path {
            remaining_path = &remaining_path[1..];

            if let Some(node) = current.tree.get(wildcard) {
                self.add_matches(
                    &node,
                    remaining_path,
                    wildcard,
                    multi_wildcard,
                    all_subscribers,
                );
            }

            if let Some(node) = current.tree.get(multi_wildcard) {
                self.add_all_children(node, all_subscribers);
            }

            if let Some(node) = current.tree.get(*elem) {
                current = node;
            } else {
                return;
            }
        }
        all_subscribers.extend(current.subscribers.clone());
    }

    pub fn add_subscriber(&mut self, pattern: &[&str], subscriber: Subscriber) {
        log::debug!("Adding subscriber for pattern {:?}", pattern);
        let mut current = &mut self.data;

        for elem in pattern {
            if !current.tree.contains_key(*elem) {
                current.tree.insert((*elem).to_owned(), Node::default());
            }
            current = current.tree.get_mut(*elem).expect("we know this exists");
        }

        current.subscribers.push(subscriber);
    }

    pub fn unsubscribe(&mut self, pattern: &[&str], subscription: Uuid) {
        let mut current = &mut self.data;

        for elem in pattern {
            if let Some(node) = current.tree.get_mut(*elem) {
                current = node;
            } else {
                log::warn!("No subscriber found for pattern {:?}", pattern);
                return;
            }
        }

        current.subscribers.retain(|s| s.id != subscription);
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

    #[test]
    fn get_subscribers() {
        let mut subscribers = Subscribers::default();

        let (tx, _rx) = unbounded_channel();
        let pattern = vec!["test", "?", "b", "#"];
        let subscriber = Subscriber::new(
            pattern.clone().into_iter().map(|s| s.to_owned()).collect(),
            tx,
        );

        subscribers.add_subscriber(&pattern, subscriber);

        let res = subscribers.get_subscribers(&["test", "a", "b", "c", "d"], "?", "#");
        assert_eq!(res.len(), 1);

        let res = subscribers.get_subscribers(&["test", "a", "b"], "?", "#");
        assert_eq!(res.len(), 0);
    }
}
