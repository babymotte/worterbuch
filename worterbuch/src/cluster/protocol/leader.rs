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

use crate::store::{SerializeableLockNode, StoreNode};
use serde::{Deserialize, Serialize};
use worterbuch_common::{
    ClientId, WorterbuchVersion,
    protocol::v1::{
        CasVersion, GraveGoods, Key, LastWill, RequestPattern, ServerMessage, Trace, Value,
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
    Set(Key, Value, bool),
    CSet(Key, Value, CasVersion, bool),
    Delete(Key),
    PDelete(RequestPattern),
}
