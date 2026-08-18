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

use serde::{Deserialize, Serialize};
use worterbuch_common::{
    ClientId, Protocol, WorterbuchVersion,
    protocol::v1::{ClientMessage, GraveGoods, Interface, Key, LastWill},
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
    pub connected_clients: Vec<Connected>,
    pub locks: Locks,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Locks {
    pub held: Vec<Key>,
    pub waiting: Vec<Key>,
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
