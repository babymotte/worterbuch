/*
 *  Cluster sync protocol definitions for leader messages
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

use crate::store::{PersistedStore, SerializeableLockNode, StoreNode};
use serde::{Deserialize, Serialize};
use worterbuch_common::{
    ClientId, WorterbuchVersion, is_grave_goods_topic, is_last_will_topic,
    protocol::v1::{
        CasVersion, ForceSet, GraveGoods, Key, LastWill, RequestPattern, SYSTEM_TOPIC_ROOT_PREFIX,
        ServerMessage, Trace, Value,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaderMessage {
    Welcome(LeaderWelcome),
    Init(StateSync),
    Mut(ClusterStateChange),
    ClientResponse(ClientId, ServerMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaderWelcome {
    pub version: WorterbuchVersion,
    pub authentication_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSync {
    pub store: StoreNode,
    pub locks: SerializeableLockNode,
    pub grave_goods: GraveGoods,
    pub last_will: LastWill,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStateChange {
    pub command: ClientWriteCommand,
    pub client_id: ClientId,
    pub trace: Trace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientWriteCommand {
    Set(Key, Value, ForceSet),
    CSet(Key, Value, CasVersion, ForceSet),
    Publish(Key, Value),
    Delete(Key),
    PDelete(RequestPattern),
    Import(PersistedStore),
}

impl ClientWriteCommand {
    pub fn is_grave_goods_or_last_will(&self) -> bool {
        match self {
            ClientWriteCommand::Set(key, _, _) | ClientWriteCommand::CSet(key, _, _, _) => {
                is_grave_goods_topic(key) || is_last_will_topic(key)
            }
            _ => false,
        }
    }

    pub fn is_system_key(&self) -> bool {
        match self.key() {
            Some(key) => key.starts_with(SYSTEM_TOPIC_ROOT_PREFIX),
            None => false,
        }
    }

    fn key(&self) -> Option<&Key> {
        match self {
            ClientWriteCommand::Set(key, _, _) => Some(key),
            ClientWriteCommand::CSet(key, _, _, _) => Some(key),
            ClientWriteCommand::Publish(key, _) => Some(key),
            ClientWriteCommand::Delete(key) => Some(key),
            ClientWriteCommand::PDelete(pattern) => Some(pattern),
            ClientWriteCommand::Import(_) => None,
        }
    }
}
