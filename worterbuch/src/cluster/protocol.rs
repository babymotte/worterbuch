/*
 *  Cluster sync protocol definitions
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
use worterbuch_common::protocol::{
    CasVersion, ClientId, ClientMessage, GraveGoods, Key, LastWill, RequestPattern, ServerMessage,
    Trace, Value, Welcome,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProxyRequest {
    ClientConnected,
    ClientDisconnected(ClientId),
    ClientRequest(ClientMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaderSyncMessage {
    Init(StateSync),
    ClientAccepted(Welcome),
    Mut(ClientWriteCommand, ClientId, Trace),
    ClientResponse(ServerMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ClientWriteCommand {
    Set(Key, Value, bool),
    CSet(Key, Value, CasVersion, bool),
    Delete(Key),
    PDelete(RequestPattern),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSync {
    pub store: StoreNode,
    pub locks: SerializeableLockNode,
    pub grave_goods: GraveGoods,
    pub last_will: LastWill,
}
