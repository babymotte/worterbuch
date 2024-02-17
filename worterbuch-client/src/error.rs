/*
 *  Worterbuch client errors module
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

use std::fmt;
use tokio::sync::broadcast;
use worterbuch_common::Err;

#[derive(Debug)]
pub enum SubscriptionError {
    RecvError(broadcast::error::RecvError),
    ServerError(Err),
    SerdeError(serde_json::Error),
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubscriptionError::RecvError(e) => e.fmt(f),
            SubscriptionError::ServerError(e) => e.fmt(f),
            SubscriptionError::SerdeError(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for SubscriptionError {}

impl From<serde_json::Error> for SubscriptionError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeError(e)
    }
}
