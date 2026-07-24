/*
 *  Worterbuch client protocol v2 implementation
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

use crate::{auth::JwtClaims, server::common::protocol::v1::V1};
use tracing::{Level, instrument};
use worterbuch_common::{error::WorterbuchResult, protocol::v1::ClientMessage};

#[derive(Clone)]
pub struct V2 {
    pub v1: V1,
}

impl V2 {
    pub fn new(v1: V1) -> Self {
        Self { v1 }
    }

    #[instrument(level=Level::TRACE, skip(self), fields(protocol = "v2", client_id=%self.v1.v0.client_id))]
    pub async fn process_incoming_message(
        &self,
        msg: ClientMessage,
        authorized: &mut Option<JwtClaims>,
    ) -> WorterbuchResult<()> {
        self.v1.process_incoming_message(msg, authorized).await
    }
}
