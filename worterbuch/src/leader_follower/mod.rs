/*
 *  Types and helper functions for leader/follower mode
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

pub(crate) mod follower;
pub(crate) mod leader;

use crate::store::StoreNode;
use serde::{Deserialize, Serialize};
use std::{
    io::{self, BufRead},
    thread,
};
use tokio::{select, sync::oneshot};
use tosub::SubsystemHandle;
use tracing::{debug, info};
use worterbuch_common::{CasVersion, GraveGoods, Key, LastWill, RequestPattern, Value};

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mode {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeaderSyncMessage {
    Init(StateSync),
    Mut(ClientWriteCommand),
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
pub struct StateSync(pub StoreNode, pub GraveGoods, pub LastWill);
