/*
 *  Helper functions for handling locks across a leader/proxy bridge
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

use crate::worterbuch::Worterbuch;
use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace};
use worterbuch_common::{
    ClientId,
    protocol::v1::{Key, TransactionId},
};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Locks {
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub held: HashMap<ClientId, HashSet<TransactionId>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub requested: HashMap<ClientId, HashSet<TransactionId>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub keys: HashMap<ClientId, HashMap<TransactionId, Key>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub tids: HashMap<ClientId, HashMap<Key, HashSet<TransactionId>>>,
}

impl Locks {
    pub fn is_empty(&self) -> bool {
        self.held.is_empty() && self.requested.is_empty()
    }

    pub fn requested(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        key: Key,
        wait: bool,
    ) {
        debug!("Client {client_id} requested lock for key {key:?}");
        if wait {
            self.requested
                .entry(client_id)
                .or_default()
                .insert(transaction_id);
        }
        self.keys
            .entry(client_id)
            .or_default()
            .insert(transaction_id, key.clone());

        self.tids
            .entry(client_id)
            .or_default()
            .entry(key)
            .or_default()
            .insert(transaction_id);

        trace!("Locks: {:#?}", self);
    }

    pub fn acquired(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        wb: &Worterbuch,
    ) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
        else {
            error!(
                "Client {} acquired lock for transaction ID {transaction_id} but was not actually waiting for a lock acquisition.",
                self.client(client_id, wb)
            );
            return;
        };
        debug!("Client {client_id} acquired lock for key {key:?}");
        if let Some(client_locks) = self.requested.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.requested.remove(&client_id);
            }
        }
        self.held
            .entry(client_id)
            .or_default()
            .insert(transaction_id);
        trace!("Locks: {:#?}", self);
    }

    pub fn acquisition_failed(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        wb: &Worterbuch,
    ) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
            .map(ToOwned::to_owned)
        else {
            error!(
                "Client {} failed to acquire lock for transaction ID {transaction_id} but was not actually waiting for a lock acquisition.",
                self.client(client_id, wb)
            );
            return;
        };
        debug!("Client {client_id} failed to acquire lock for key {key:?}");
        if let Some(client_locks) = self.requested.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.requested.remove(&client_id);
            }
        }
        if let Some(client_keys) = self.keys.get_mut(&client_id) {
            client_keys.remove(&transaction_id);
            if client_keys.is_empty() {
                self.keys.remove(&client_id);
            }
        }
        if let Some(client_tids) = self.tids.get_mut(&client_id) {
            if let Some(tids) = client_tids.get_mut(&key) {
                tids.remove(&transaction_id);
                if tids.is_empty() {
                    client_tids.remove(&key);
                }
            }
            if client_tids.is_empty() {
                self.tids.remove(&client_id);
            }
        }
        trace!("Locks: {:#?}", self);
    }

    pub fn released(
        &mut self,
        client_id: ClientId,
        key: Key,
        wb: &Worterbuch,
    ) -> Option<HashSet<TransactionId>> {
        let Some(keys) = self.tids.get_mut(&client_id) else {
            error!(
                "Client {} released lock for key {key:?} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return None;
        };

        let Some(tids) = keys.remove(&key) else {
            error!(
                "Client {} released lock for key {key:?} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return None;
        };

        if keys.is_empty() {
            self.tids.remove(&client_id);
        }

        for tid in &tids {
            self.released_tid(client_id, *tid, wb);
        }

        Some(tids)
    }

    fn released_tid(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        wb: &Worterbuch,
    ) {
        let Some(tids) = self.keys.get_mut(&client_id) else {
            error!(
                "Client {} released lock for transaction ID {transaction_id} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return;
        };

        let Some(key) = tids.remove(&transaction_id) else {
            error!(
                "Client {} released lock for transaction ID {transaction_id} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return;
        };

        if tids.is_empty() {
            self.keys.remove(&client_id);
        }

        debug!("Client {client_id} released lock on key {key:?}");

        if let Some(held_client_locks) = self.held.get_mut(&client_id) {
            held_client_locks.remove(&transaction_id);
            if held_client_locks.is_empty() {
                self.held.remove(&client_id);
            }
        }
        if let Some(requested_client_locks) = self.requested.get_mut(&client_id) {
            requested_client_locks.remove(&transaction_id);
            if requested_client_locks.is_empty() {
                self.requested.remove(&client_id);
            }
        }

        if let Some(client_keys) = self.keys.get_mut(&client_id) {
            client_keys.remove(&transaction_id);
            if client_keys.is_empty() {
                self.keys.remove(&client_id);
            }
        }

        trace!("Locks: {:#?}", self);
    }

    pub fn release_failed(&mut self, client_id: ClientId, key: Key, wb: &Worterbuch) {
        let Some(keys) = self.tids.get_mut(&client_id) else {
            // this is kind of expected since we already clean up before sending the release command to the server
            debug!(
                "Client {} failed to release lock for key {key:?} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return;
        };

        let Some(tids) = keys.remove(&key) else {
            // this is kind of expected since we already clean up before sending the release command to the server
            debug!(
                "Client {} failed to release lock for key {key:?} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return;
        };

        if keys.is_empty() {
            self.tids.remove(&client_id);
        }

        for tid in &tids {
            self.release_tid_failed(client_id, *tid, wb);
        }
    }

    fn release_tid_failed(
        &mut self,
        client_id: ClientId,
        transaction_id: TransactionId,
        wb: &Worterbuch,
    ) {
        let Some(tids) = self.keys.get_mut(&client_id) else {
            error!(
                "Client {} failed to release lock for transaction ID {transaction_id} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return;
        };

        let Some(key) = tids.remove(&transaction_id) else {
            error!(
                "Client {} failed to release lock for transaction ID {transaction_id} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return;
        };

        if tids.is_empty() {
            self.keys.remove(&client_id);
        }

        debug!("Client {client_id} failed to release lock for key {key:?}");

        if let Some(client_locks) = self.held.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.held.remove(&client_id);
            }
        }

        if let Some(client_locks) = self.requested.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.requested.remove(&client_id);
            }
        }

        if let Some(client_keys) = self.keys.get_mut(&client_id) {
            client_keys.remove(&transaction_id);
            if client_keys.is_empty() {
                self.keys.remove(&client_id);
            }
        }

        trace!("Locks: {:#?}", self);
    }

    pub fn lost(&mut self, client_id: ClientId, transaction_id: TransactionId, wb: &Worterbuch) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
            .map(ToOwned::to_owned)
        else {
            error!(
                "Client {} lost lock for transaction ID {transaction_id} but was not actually holding or waiting for a lock.",
                self.client(client_id, wb)
            );
            return;
        };
        debug!("Client {client_id} lost lock on key {key:?}");
        if let Some(client_locks) = self.held.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.held.remove(&client_id);
            }
        }
        if let Some(client_locks) = self.requested.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.requested.remove(&client_id);
            }
        }
        if let Some(client_keys) = self.keys.get_mut(&client_id) {
            client_keys.remove(&transaction_id);
            if client_keys.is_empty() {
                self.keys.remove(&client_id);
            }
        }
        if let Some(client_tids) = self.tids.get_mut(&client_id) {
            if let Some(tids) = client_tids.get_mut(&key) {
                tids.remove(&transaction_id);
                if tids.is_empty() {
                    client_tids.remove(&key);
                }
            }
            if client_tids.is_empty() {
                self.tids.remove(&client_id);
            }
        }
        trace!("Locks: {:#?}", self);
    }

    pub fn client_disconnected(&mut self, client_id: ClientId) {
        debug!("Client {client_id} disconnected, clearing held and requested locks.");
        self.requested.remove(&client_id);
        self.held.remove(&client_id);
        self.keys.remove(&client_id);
        self.tids.remove(&client_id);
        trace!("Locks: {:#?}", self);
    }

    fn client(&self, client_id: ClientId, wb: &Worterbuch) -> String {
        if let Some(name) = wb.client_name(client_id) {
            format!("{} ({})", client_id, name)
        } else {
            client_id.to_string()
        }
    }
}

#[cfg(test)]
mod test {

    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::{INTERNAL_CLIENT_ID, config::Config};
    use serde_json::json;
    use uuid::Uuid;
    use worterbuch_common::{
        protocol::v1::{
            Interface, SYSTEM_TOPIC_CLIENT_NAME, SYSTEM_TOPIC_CLIENTS, SYSTEM_TOPIC_ROOT, TraceData,
        },
        topic,
    };

    trait ToVec {
        type Item;
        fn to_vec(self) -> Vec<Self::Item>;
    }

    impl<T: Ord> ToVec for HashSet<T> {
        type Item = T;

        fn to_vec(self) -> Vec<Self::Item> {
            let mut vec = self.into_iter().collect::<Vec<_>>();
            vec.sort_unstable();
            vec
        }
    }

    async fn worterbuch() -> Worterbuch {
        let _ = dotenvy::dotenv();
        Worterbuch::with_config(Config::new(None).await.unwrap())
    }

    fn client() -> ClientId {
        Uuid::new_v4()
    }

    fn key(k: &str) -> Key {
        k.to_owned()
    }

    fn is_requested(locks: &Locks, client_id: ClientId, tid: TransactionId) -> bool {
        locks
            .requested
            .get(&client_id)
            .is_some_and(|t| t.contains(&tid))
    }

    fn is_held(locks: &Locks, client_id: ClientId, tid: TransactionId) -> bool {
        locks.held.get(&client_id).is_some_and(|t| t.contains(&tid))
    }

    fn key_of(locks: &Locks, client_id: ClientId, tid: TransactionId) -> Option<&Key> {
        locks.keys.get(&client_id).and_then(|k| k.get(&tid))
    }

    fn tids_of(locks: &Locks, client_id: ClientId, key: &str) -> Vec<TransactionId> {
        locks
            .tids
            .get(&client_id)
            .and_then(|k| k.get(key))
            .cloned()
            .unwrap_or_default()
            .to_vec()
    }

    /// Asserts that no empty inner collections are left behind and that `keys` and `tids`
    /// are exact inverses of each other.
    fn assert_consistent(locks: &Locks) {
        assert!(locks.held.values().all(|s| !s.is_empty()), "{locks:?}");
        assert!(locks.requested.values().all(|s| !s.is_empty()), "{locks:?}");
        assert!(locks.keys.values().all(|m| !m.is_empty()), "{locks:?}");
        assert!(locks.tids.values().all(|m| !m.is_empty()), "{locks:?}");
        assert!(
            locks
                .tids
                .values()
                .all(|m| m.values().all(|v| !v.is_empty())),
            "{locks:?}"
        );

        for (client_id, keys) in &locks.keys {
            for (tid, key) in keys {
                assert!(
                    tids_of(locks, *client_id, key).contains(tid),
                    "tid {tid} of client {client_id} for key {key} missing in tids: {locks:?}"
                );
            }
        }
        for (client_id, keys) in &locks.tids {
            for (key, tids) in keys {
                for tid in tids {
                    assert_eq!(
                        key_of(locks, *client_id, *tid),
                        Some(key),
                        "tid {tid} of client {client_id} for key {key} missing in keys: {locks:?}"
                    );
                }
            }
        }

        // every held or requested lock must have a known key
        for (client_id, tids) in locks.held.iter().chain(locks.requested.iter()) {
            for tid in tids {
                assert!(
                    key_of(locks, *client_id, *tid).is_some(),
                    "held/requested tid {tid} of client {client_id} has no key: {locks:?}"
                );
            }
        }
    }

    fn assert_fully_empty(locks: &Locks) {
        assert!(locks.is_empty(), "{locks:?}");
        assert!(locks.held.is_empty(), "{locks:?}");
        assert!(locks.requested.is_empty(), "{locks:?}");
        assert!(locks.keys.is_empty(), "{locks:?}");
        assert!(locks.tids.is_empty(), "{locks:?}");
    }

    // ---------------------------------------------------------------------------------------
    // is_empty
    // ---------------------------------------------------------------------------------------

    #[test]
    fn default_is_empty() {
        let locks = Locks::default();
        assert_fully_empty(&locks);
    }

    #[test]
    fn waiting_request_makes_non_empty() {
        let mut locks = Locks::default();
        locks.requested(client(), 1, key("a/b"), true);
        assert!(!locks.is_empty());
    }

    #[test]
    fn non_waiting_request_keeps_empty() {
        let mut locks = Locks::default();
        locks.requested(client(), 1, key("a/b"), false);
        // a try-lock that has not yet been granted is neither held nor requested
        assert!(locks.is_empty());
    }

    // ---------------------------------------------------------------------------------------
    // requested
    // ---------------------------------------------------------------------------------------

    #[test]
    fn requested_with_wait_tracks_request_and_key() {
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);

        assert!(is_requested(&locks, c, 1));
        assert!(!is_held(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), Some(&key("a/b")));
    }

    #[test]
    fn requested_without_wait_tracks_only_key() {
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), false);

        assert!(!is_requested(&locks, c, 1));
        assert!(!is_held(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), Some(&key("a/b")));
    }

    #[test]
    fn requested_populates_tids() {
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.requested(c, 2, key("a/b"), false);
        locks.requested(c, 3, key("c/d"), true);

        assert_eq!(tids_of(&locks, c, "a/b"), vec![1, 2]);
        assert_eq!(tids_of(&locks, c, "c/d"), vec![3]);
        assert_consistent(&locks);
    }

    #[test]
    fn requests_of_different_clients_are_isolated() {
        let mut locks = Locks::default();
        let c1 = client();
        let c2 = client();
        locks.requested(c1, 1, key("a/b"), true);
        locks.requested(c2, 1, key("c/d"), true);

        assert_eq!(key_of(&locks, c1, 1), Some(&key("a/b")));
        assert_eq!(key_of(&locks, c2, 1), Some(&key("c/d")));
        assert_eq!(locks.requested.len(), 2);
        assert_eq!(tids_of(&locks, c1, "a/b"), vec![1]);
        assert_eq!(tids_of(&locks, c2, "c/d"), vec![1]);
        assert!(tids_of(&locks, c1, "c/d").is_empty());
        assert!(tids_of(&locks, c2, "a/b").is_empty());
    }

    // ---------------------------------------------------------------------------------------
    // acquired
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn acquired_moves_request_to_held() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);

        assert!(!is_requested(&locks, c, 1));
        assert!(is_held(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), Some(&key("a/b")));
        assert!(!locks.requested.contains_key(&c));
        assert_eq!(tids_of(&locks, c, "a/b"), vec![1]);
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn acquired_after_non_waiting_request() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), false);
        locks.acquired(c, 1, &wb);

        assert!(is_held(&locks, c, 1));
        assert!(locks.requested.is_empty());
        assert!(!locks.is_empty());
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn acquired_keeps_other_pending_requests() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.requested(c, 2, key("c/d"), true);
        locks.acquired(c, 1, &wb);

        assert!(is_held(&locks, c, 1));
        assert!(is_requested(&locks, c, 2));
        assert!(!is_held(&locks, c, 2));
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn acquired_without_request_is_ignored() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.acquired(c, 1, &wb);
        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn acquired_with_unknown_tid_is_ignored() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 2, &wb);

        assert!(is_requested(&locks, c, 1));
        assert!(locks.held.is_empty());
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn acquired_for_other_client_is_ignored() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c1 = client();
        let c2 = client();
        locks.requested(c1, 1, key("a/b"), true);
        locks.acquired(c2, 1, &wb);

        assert!(is_requested(&locks, c1, 1));
        assert!(locks.held.is_empty());
        assert_consistent(&locks);
    }

    // ---------------------------------------------------------------------------------------
    // acquisition_failed
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn acquisition_failed_clears_request() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquisition_failed(c, 1, &wb);

        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn acquisition_failed_after_non_waiting_request_clears_key() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), false);
        locks.acquisition_failed(c, 1, &wb);

        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn acquisition_failed_keeps_other_transactions() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.requested(c, 2, key("a/b"), true);
        locks.requested(c, 3, key("c/d"), true);
        locks.acquired(c, 3, &wb);
        locks.acquisition_failed(c, 1, &wb);

        assert!(!is_requested(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), None);
        assert!(is_requested(&locks, c, 2));
        assert!(is_held(&locks, c, 3));
        assert_eq!(tids_of(&locks, c, "a/b"), vec![2]);
        assert_eq!(tids_of(&locks, c, "c/d"), vec![3]);
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn acquisition_failed_without_request_is_ignored() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquisition_failed(c, 2, &wb);
        locks.acquisition_failed(client(), 1, &wb);

        assert!(is_requested(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), Some(&key("a/b")));
        assert_consistent(&locks);
    }

    // ---------------------------------------------------------------------------------------
    // released
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn released_returns_tid_and_clears_lock() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);

        let tids = locks.released(c, key("a/b"), &wb).map(ToVec::to_vec);

        assert_eq!(tids, Some(vec![1]));
        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn released_returns_all_tids_for_key() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        // one held lock plus one queued request on the same key
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.requested(c, 2, key("a/b"), true);

        let tids = locks.released(c, key("a/b"), &wb).unwrap().to_vec();

        assert_eq!(tids, vec![1, 2]);
        assert!(!is_held(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), None);
        assert_eq!(key_of(&locks, c, 2), None);
        assert!(locks.tids.is_empty());
    }

    #[tokio::test]
    async fn released_keeps_locks_on_other_keys() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.requested(c, 2, key("c/d"), true);
        locks.acquired(c, 2, &wb);

        let tids = locks.released(c, key("a/b"), &wb).map(ToVec::to_vec);

        assert_eq!(tids, Some(vec![1]));
        assert!(!is_held(&locks, c, 1));
        assert!(is_held(&locks, c, 2));
        assert_eq!(key_of(&locks, c, 2), Some(&key("c/d")));
        assert!(tids_of(&locks, c, "a/b").is_empty());
        assert_eq!(tids_of(&locks, c, "c/d"), vec![2]);
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn released_keeps_locks_of_other_clients() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c1 = client();
        let c2 = client();
        locks.requested(c1, 1, key("a/b"), true);
        locks.acquired(c1, 1, &wb);
        locks.requested(c2, 1, key("a/b"), true);

        let tids = locks.released(c1, key("a/b"), &wb).map(ToVec::to_vec);

        assert_eq!(tids, Some(vec![1]));
        assert!(locks.held.is_empty());
        assert!(is_requested(&locks, c2, 1));
        assert_eq!(tids_of(&locks, c2, "a/b"), vec![1]);
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn released_twice_returns_none() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);

        assert!(locks.released(c, key("a/b"), &wb).is_some());
        assert_eq!(locks.released(c, key("a/b"), &wb), None);
        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn released_unknown_client_returns_none() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        assert_eq!(locks.released(client(), key("a/b"), &wb), None);
        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn released_unknown_key_returns_none() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);

        assert_eq!(locks.released(c, key("c/d"), &wb), None);
        assert!(is_held(&locks, c, 1));
        assert_consistent(&locks);
    }

    // ---------------------------------------------------------------------------------------
    // release_failed
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn release_failed_clears_held_lock() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.released(c, key("a/b"), &wb);
        locks.release_failed(c, key("a/b"), &wb);

        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn release_failed_keeps_other_locks() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.requested(c, 2, key("c/d"), true);
        locks.acquired(c, 2, &wb);
        locks.released(c, key("a/b"), &wb);
        locks.release_failed(c, key("a/b"), &wb);

        assert!(!is_held(&locks, c, 1));
        assert!(is_held(&locks, c, 2));
        assert!(tids_of(&locks, c, "a/b").is_empty());
        assert_eq!(tids_of(&locks, c, "c/d"), vec![2]);
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn release_failed_unknown_key_is_ignored() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.released(c, key("c/d"), &wb);
        locks.release_failed(c, key("c/d"), &wb);
        locks.released(client(), key("a/b"), &wb);
        locks.release_failed(client(), key("a/b"), &wb);

        assert!(is_held(&locks, c, 1));
        assert_consistent(&locks);
    }

    // ---------------------------------------------------------------------------------------
    // lost
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn lost_clears_held_lock() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.lost(c, 1, &wb);

        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn lost_keeps_other_locks() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.requested(c, 2, key("a/b"), true);
        locks.lost(c, 1, &wb);

        assert!(!is_held(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), None);
        assert!(is_requested(&locks, c, 2));
        assert_eq!(tids_of(&locks, c, "a/b"), vec![2]);
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn lost_unknown_tid_is_ignored() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.lost(c, 2, &wb);
        locks.lost(client(), 1, &wb);

        assert!(is_held(&locks, c, 1));
        assert_consistent(&locks);
    }

    // ---------------------------------------------------------------------------------------
    // client_disconnected
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn client_disconnected_clears_everything_of_client() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        locks.requested(c, 2, key("c/d"), true);
        locks.requested(c, 3, key("e/f"), false);

        locks.client_disconnected(c);

        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn client_disconnected_keeps_other_clients() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c1 = client();
        let c2 = client();
        locks.requested(c1, 1, key("a/b"), true);
        locks.acquired(c1, 1, &wb);
        locks.requested(c2, 1, key("a/b"), true);

        locks.client_disconnected(c1);

        assert!(!locks.held.contains_key(&c1));
        assert!(!locks.keys.contains_key(&c1));
        assert!(!locks.tids.contains_key(&c1));
        assert!(is_requested(&locks, c2, 1));
        assert_eq!(tids_of(&locks, c2, "a/b"), vec![1]);
        assert_consistent(&locks);
    }

    #[test]
    fn client_disconnected_unknown_client_is_noop() {
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.client_disconnected(client());

        assert!(is_requested(&locks, c, 1));
        assert_consistent(&locks);
    }

    // ---------------------------------------------------------------------------------------
    // full lifecycles
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn tid_can_be_reused_after_release() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);
        assert_eq!(
            locks.released(c, key("a/b"), &wb).map(ToVec::to_vec),
            Some(vec![1])
        );

        locks.requested(c, 1, key("c/d"), true);
        locks.acquired(c, 1, &wb);

        assert!(is_held(&locks, c, 1));
        assert_eq!(key_of(&locks, c, 1), Some(&key("c/d")));
        assert!(tids_of(&locks, c, "a/b").is_empty());
        assert_eq!(tids_of(&locks, c, "c/d"), vec![1]);
        assert_consistent(&locks);
    }

    #[tokio::test]
    async fn interleaved_lifecycle_ends_empty() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c1 = client();
        let c2 = client();

        locks.requested(c1, 1, key("a"), true);
        locks.requested(c1, 2, key("b"), false);
        locks.requested(c2, 1, key("a"), true);
        locks.requested(c2, 2, key("c"), true);
        assert_consistent(&locks);

        locks.acquired(c1, 1, &wb);
        locks.acquisition_failed(c1, 2, &wb);
        locks.acquired(c2, 2, &wb);
        assert_consistent(&locks);

        assert_eq!(
            locks.released(c1, key("a"), &wb).map(ToVec::to_vec),
            Some(vec![1])
        );
        assert_consistent(&locks);

        locks.acquired(c2, 1, &wb);
        locks.lost(c2, 1, &wb);
        locks.released(c2, key("c"), &wb);
        locks.release_failed(c2, key("c"), &wb);

        assert_fully_empty(&locks);
    }

    // ---------------------------------------------------------------------------------------
    // client name formatting
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn client_without_name_is_formatted_as_id() {
        let wb = worterbuch().await;
        let locks = Locks::default();
        let c = client();
        assert_eq!(locks.client(c, &wb), c.to_string());
    }

    #[tokio::test]
    async fn client_with_name_is_formatted_with_name() {
        let mut wb = worterbuch().await;
        let locks = Locks::default();
        let c = client();
        wb.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                c,
                SYSTEM_TOPIC_CLIENT_NAME
            ),
            json!("my-client"),
            false,
            TraceData::new(INTERNAL_CLIENT_ID, Interface::Local, 1),
        )
        .await
        .unwrap();

        assert_eq!(locks.client(c, &wb), format!("{c} (my-client)"));
    }

    // ---------------------------------------------------------------------------------------
    // serialization
    // ---------------------------------------------------------------------------------------

    #[test]
    fn empty_locks_serialize_to_empty_object() {
        let locks = Locks::default();
        assert_eq!(serde_json::to_string(&locks).unwrap(), "{}");
    }

    #[test]
    fn empty_object_deserializes_to_empty_locks() {
        let locks: Locks = serde_json::from_str("{}").unwrap();
        assert_fully_empty(&locks);
    }

    #[tokio::test]
    async fn serialization_uses_camel_case_and_skips_empty_maps() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);

        let value = serde_json::to_value(&locks).unwrap();
        let obj = value.as_object().unwrap();

        assert!(obj.contains_key("held"));
        assert!(!obj.contains_key("requested"));
        assert!(obj.contains_key("keys"));
        assert!(obj.contains_key("tids"));
        assert_eq!(value["held"][c.to_string()], json!([1]));
        assert_eq!(value["keys"][c.to_string()]["1"], json!("a/b"));
        assert_eq!(value["tids"][c.to_string()]["a/b"], json!([1]));
    }

    #[tokio::test]
    async fn serialization_roundtrip() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c1 = client();
        let c2 = client();
        locks.requested(c1, 1, key("a/b"), true);
        locks.acquired(c1, 1, &wb);
        locks.requested(c1, 2, key("a/b"), true);
        locks.requested(c2, 7, key("c/d"), false);

        let json = serde_json::to_string(&locks).unwrap();
        let restored: Locks = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.held, locks.held);
        assert_eq!(restored.requested, locks.requested);
        assert_eq!(restored.keys, locks.keys);
        assert_eq!(restored.tids, locks.tids);
    }

    #[tokio::test]
    async fn deserialized_locks_remain_operational() {
        let wb = worterbuch().await;
        let mut locks = Locks::default();
        let c = client();
        locks.requested(c, 1, key("a/b"), true);
        locks.acquired(c, 1, &wb);

        let json = serde_json::to_string(&locks).unwrap();
        let mut restored: Locks = serde_json::from_str(&json).unwrap();

        assert_eq!(
            restored.released(c, key("a/b"), &wb).map(ToVec::to_vec),
            Some(vec![1])
        );
        assert_fully_empty(&restored);
    }
}
