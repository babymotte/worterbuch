/*
 *  Cluster sync protocol definitions for proxy messages
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

use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, trace};
use worterbuch_common::{
    ClientId, Protocol, WorterbuchVersion,
    protocol::v1::{ClientMessage, GraveGoods, Interface, Key, LastWill, TransactionId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProxyMessage {
    Handshake(Handshake),
    Connected(Connected),
    Disconnected(Disconnected),
    Request(Request),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Handshake {
    Proxy(ProxyHandshake),
    Follower(FollowerHandshake),
}
impl Handshake {
    pub(crate) fn version(&self) -> &WorterbuchVersion {
        match self {
            Handshake::Proxy(proxy_handshake) => &proxy_handshake.version,
            Handshake::Follower(follower_handshake) => &follower_handshake.version,
        }
    }

    pub(crate) fn auth_token(&self) -> Option<&str> {
        match self {
            Handshake::Proxy(proxy_handshake) => proxy_handshake.auth_token.as_deref(),
            Handshake::Follower(follower_handshake) => follower_handshake.auth_token.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowerHandshake {
    pub version: WorterbuchVersion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyHandshake {
    pub version: WorterbuchVersion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub connected_clients: Vec<Connected>,
    #[serde(skip_serializing_if = "Locks::is_empty", default)]
    pub locks: Locks,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Locks {
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub held: HashMap<ClientId, HashSet<TransactionId>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub requested: HashMap<ClientId, HashSet<TransactionId>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub keys: HashMap<ClientId, HashMap<TransactionId, Key>>,
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
            .insert(transaction_id, key);
        trace!("Locks: {:?}", self);
    }

    pub fn acquired(&mut self, client_id: ClientId, transaction_id: TransactionId) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
        else {
            error!("No key found for transaction_id {transaction_id} for client {client_id}");
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
        trace!("Locks: {:?}", self);
    }

    pub fn acquisition_failed(&mut self, client_id: ClientId, transaction_id: TransactionId) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
        else {
            error!("No key found for transaction_id {transaction_id} for client {client_id}");
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
        trace!("Locks: {:?}", self);
    }

    pub fn released(&mut self, client_id: ClientId, transaction_id: TransactionId) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
        else {
            error!("No key found for transaction_id {transaction_id} for client {client_id}");
            return;
        };
        debug!("Client {client_id} released lock on key {key:?}");
        if let Some(client_locks) = self.held.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.held.remove(&client_id);
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

    pub fn release_failed(&mut self, client_id: ClientId, transaction_id: TransactionId) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
        else {
            error!("No key found for transaction_id {transaction_id} for client {client_id}");
            return;
        };
        debug!("Client {client_id} failed to release lock for key {key:?}");
        if let Some(client_locks) = self.held.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.held.remove(&client_id);
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

    pub fn lost(&mut self, client_id: ClientId, transaction_id: TransactionId) {
        let Some(key) = self
            .keys
            .get(&client_id)
            .and_then(|tids| tids.get(&transaction_id))
        else {
            error!("No key found for transaction_id {transaction_id} for client {client_id}");
            return;
        };
        debug!("Client {client_id} lost lock on key {key:?}");
        if let Some(client_locks) = self.held.get_mut(&client_id) {
            client_locks.remove(&transaction_id);
            if client_locks.is_empty() {
                self.held.remove(&client_id);
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

    pub fn client_disconnected(&mut self, client_id: ClientId) {
        debug!("Client {client_id} disconnected, clearing held and requested locks.");
        self.requested.remove(&client_id);
        self.held.remove(&client_id);
        self.keys.remove(&client_id);
        trace!("Locks: {:#?}", self);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Connected {
    pub client_id: ClientId,
    pub protocol: Protocol,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Disconnected {
    pub client_id: ClientId,
    pub protocol: Protocol,
    pub grave_goods: GraveGoods,
    pub last_will: LastWill,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub client_id: ClientId,
    pub msg: ClientMessage,
    pub interface: Interface,
}
