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
    ClientId, Protocol,
    protocol::v1::{ClientMessage, Interface, ProtocolMajorVersion},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProxyMessage {
    #[serde(rename_all = "camelCase")]
    Connected {
        client_id: ClientId,
        protocol: Protocol,
    },
    #[serde(rename_all = "camelCase")]
    Disconnected {
        client_id: ClientId,
        protocol: Protocol,
    },
    #[serde(rename_all = "camelCase")]
    ProtocolSwitched {
        client_id: ClientId,
        interface: Interface,
        version: ProtocolMajorVersion,
    },
    #[serde(rename_all = "camelCase")]
    Request {
        client_id: ClientId,
        msg: ClientMessage,
        interface: Interface,
    },
}
