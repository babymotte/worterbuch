use std::collections::HashMap;

use serde::{Deserialize, Serialize};

type Value = Option<String>;
type Tree = HashMap<String, Node>;

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Node {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Value,
    pub t: Tree,
}

impl Node {
    fn merge(
        &mut self,
        other: Node,
        key: Option<&str>,
        insertions: &mut Vec<(String, String)>,
        path: &[&str],
        separator: char,
    ) {
        if let Some(v) = other.v {
            self.v = Some(v.clone());
            let key = concat_key(&path, key, separator);
            log::debug!("Imported {} = {}", key, v);
            insertions.push((key, v));
        }

        let mut path = path.to_owned();
        if let Some(key) = key {
            path.push(key);
        }

        for (key, node) in other.t {
            if !self.t.contains_key(&key) {
                self.t.insert(key.clone(), Node::default());
            }
            let own_node = self.t.get_mut(&key).expect("we just checked for presence");
            own_node.merge(node, Some(&key), insertions, &path, separator);
        }
    }
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreStats {
    num_entries: usize,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Store {
    data: Node,
}

impl Store {
    /// retrieve a value for a non-wildcard key
    pub fn get(&self, path: &[&str]) -> Option<&str> {
        let mut current = &self.data;

        for elem in path {
            if let Some(node) = current.t.get(*elem) {
                current = node;
            } else {
                return None;
            }
        }

        current.v.as_deref()
    }

    /// retrieve values for a key containing at least one single-level wildcard but no multi-level wildcard
    pub fn get_matches(
        &self,
        path: &[&str],
        wildcard: &str,
        separator: &str,
    ) -> Vec<(String, String)> {
        let mut matches = Vec::new();
        let traversed = vec![];
        self.collect_matches(
            &self.data,
            traversed,
            path,
            &mut matches,
            wildcard,
            separator,
            false,
        );
        matches
    }

    fn collect_matches<'p>(
        &self,
        current_node: &Node,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &[&'p str],
        matches: &mut Vec<(String, String)>,
        wildcard: &str,
        separator: &str,
        children: bool,
    ) {
        let mut current = current_node;
        let mut remaining_path = remaining_path;

        for elem in remaining_path {
            remaining_path = &remaining_path[1..];
            if elem == &wildcard {
                for (key, node) in &current.t {
                    let mut traversed_path = traversed_path.clone();
                    traversed_path.push(key);
                    self.collect_matches(
                        node,
                        traversed_path,
                        remaining_path,
                        matches,
                        wildcard,
                        separator,
                        children,
                    );
                }
                return;
            } else {
                if let Some(node) = current.t.get(*elem) {
                    traversed_path.push(*elem);
                    current = node;
                } else {
                    return;
                }
            }
        }

        if children {
            self.collect_all_children(current, traversed_path.clone(), matches, separator);
        } else {
            if let Some(val) = &current.v {
                let key = traversed_path.join(separator);
                matches.push((key, val.to_owned()));
            }
        }
    }

    /// retrieve values for a key ending with a multi-level wildcard but no single level wildcard
    pub fn get_children(&self, path: &[&str], separator: &str) -> Vec<(String, String)> {
        let mut children = Vec::new();
        let traversed = vec![];
        self.collect_children(&self.data, traversed, path, &mut children, separator);
        children
    }

    fn collect_children<'p>(
        &self,
        current_node: &Node,
        mut traversed_path: Vec<&'p str>,
        remaining_path: &[&'p str],
        matches: &mut Vec<(String, String)>,
        separator: &str,
    ) {
        let mut current = current_node;
        let mut remaining_path = remaining_path;

        for elem in remaining_path {
            remaining_path = &remaining_path[1..];

            if let Some(node) = current.t.get(*elem) {
                traversed_path.push(*elem);
                current = node;
            } else {
                return;
            }
        }

        self.collect_all_children(current, traversed_path.clone(), matches, separator);
    }

    fn collect_all_children<'p>(
        &self,
        current: &Node,
        traversed_path: Vec<&str>,
        matches: &mut Vec<(String, String)>,
        separator: &str,
    ) {
        for (key, node) in &current.t {
            let mut traversed_path = traversed_path.clone();
            traversed_path.push(key);

            if let Some(val) = &node.v {
                let key = traversed_path.join(separator);
                matches.push((key, val.to_owned()));
            }

            self.collect_all_children(node, traversed_path, matches, separator);
        }
    }

    /// retrieve values for a key ending with a multi-level wildcard that also contains at least one single level wildcard
    pub fn get_match_children(
        &self,
        path: &[&str],
        wildcard: &str,
        separator: &str,
    ) -> Vec<(String, String)> {
        let mut matches = Vec::new();
        let traversed = vec![];
        self.collect_matches(
            &self.data,
            traversed,
            path,
            &mut matches,
            wildcard,
            separator,
            true,
        );
        matches
    }

    pub fn insert(&mut self, path: &[&str], value: String) {
        let mut current = &mut self.data;

        for elem in path {
            if !current.t.contains_key(*elem) {
                current.t.insert((*elem).to_owned(), Node::default());
            }
            current = current.t.get_mut(*elem).expect("we know this exists");
        }

        current.v = Some(value)
    }

    pub fn merge(&mut self, other: Store, separator: char) -> Vec<(String, String)> {
        let mut insertions = Vec::new();
        let path = Vec::new();
        self.data
            .merge(other.data, None, &mut insertions, &path, separator);
        insertions
    }

    #[cfg(not(feature = "docker"))]
    pub fn stats(&self) -> StoreStats {
        let num_entries = self.count_entries();

        StoreStats { num_entries }
    }

    #[cfg(not(feature = "docker"))]
    fn count_entries(&self) -> usize {
        count_children(&self.data)
    }
}

fn concat_key(path: &[&str], key: Option<&str>, separator: char) -> String {
    let mut string = String::new();
    for elem in path {
        string.push_str(elem);
        string.push(separator);
    }
    if let Some(key) = key {
        string.push_str(key);
    }
    string
}

#[cfg(not(feature = "docker"))]
fn count_children(node: &Node) -> usize {
    let mut count = 0;

    if node.v.is_some() {
        count += 1;
    }

    for child in node.t.values() {
        count += count_children(child);
    }

    count
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_insert_get() {
        let path = vec!["test", "a", "b"];

        let mut store = Store::default();
        store.insert(&path, "Hello, World!".to_owned());
        assert_eq!(store.get(&path), Some("Hello, World!"));

        assert_eq!(store.get(&vec!["test", "a"]), None);
        assert_eq!(store.get(&vec!["test", "a", "b", "c"]), None);
    }

    #[test]
    fn test_wildcard() {
        let path0 = vec!["trolo", "a"];
        let path1 = vec!["test", "a", "b"];
        let path2 = vec!["test", "a", "c"];
        let path3 = vec!["trolo", "a", "b"];
        let path4 = vec!["trolo", "c", "b"];
        let path5 = vec!["trolo", "c", "b", "d"];

        let mut store = Store::default();
        store.insert(&path0, "0".to_owned());
        store.insert(&path1, "1".to_owned());
        store.insert(&path2, "2".to_owned());
        store.insert(&path3, "3".to_owned());
        store.insert(&path4, "4".to_owned());
        store.insert(&path5, "5".to_owned());

        let res = store.get_matches(&vec!["test", "a", "?"], "?", "/");
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), "1".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/c".to_owned(), "2".to_owned()))
            .is_some());

        let res = store.get_matches(&vec!["trolo", "?", "b"], "?", "/");
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), "3".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), "4".to_owned()))
            .is_some());

        let res = store.get_matches(&vec!["?", "a", "b"], "?", "/");
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), "1".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), "3".to_owned()))
            .is_some());

        let res = store.get_matches(&vec!["?", "?", "b"], "?", "/");
        assert_eq!(res.len(), 3);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), "1".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), "3".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), "4".to_owned()))
            .is_some());
    }

    #[test]
    fn test_multi_wildcard() {
        let path0 = vec!["trolo", "a"];
        let path1 = vec!["test", "a", "b"];
        let path2 = vec!["test", "a", "c"];
        let path3 = vec!["trolo", "a", "b"];
        let path4 = vec!["trolo", "c", "b"];
        let path5 = vec!["trolo", "c", "b", "d"];

        let mut store = Store::default();
        store.insert(&path0, "0".to_owned());
        store.insert(&path1, "1".to_owned());
        store.insert(&path2, "2".to_owned());
        store.insert(&path3, "3".to_owned());
        store.insert(&path4, "4".to_owned());
        store.insert(&path5, "5".to_owned());

        let res = store.get_children(&vec!["test", "a"], "/");
        assert_eq!(res.len(), 2);
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), "1".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/c".to_owned(), "2".to_owned()))
            .is_some());

        let res = store.get_children(&vec!["trolo"], "/");
        assert_eq!(res.len(), 4);
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a".to_owned(), "0".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), "3".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), "4".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b/d".to_owned(), "5".to_owned()))
            .is_some());

        let res = store.get_children(&vec![], "/");
        assert_eq!(res.len(), 6);
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a".to_owned(), "0".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/b".to_owned(), "1".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("test/a/c".to_owned(), "2".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/a/b".to_owned(), "3".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b".to_owned(), "4".to_owned()))
            .is_some());
        assert!(res
            .iter()
            .find(|e| e == &&("trolo/c/b/d".to_owned(), "5".to_owned()))
            .is_some());
    }
}
