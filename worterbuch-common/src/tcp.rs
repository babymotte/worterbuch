/*
 *  Worterbuch TCP utils module
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

use crate::error::{ConnectionError, ConnectionResult};
use serde::Serialize;
use std::io;
use tokio::io::AsyncWriteExt;

pub async fn write_line_and_flush(
    msg: impl Serialize,
    mut tx: impl AsyncWriteExt + Unpin,
) -> ConnectionResult<()> {
    let json = serde_json::to_string(&msg)?;
    if json.contains('\n') {
        return Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid JSON: '{json}' contains line break"),
        )));
    }
    if json.trim().is_empty() {
        return Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid JSON: '{json}' is empty"),
        )));
    }
    log::debug!("Sending message: {json}");
    log::trace!("Writing line …");
    tx.write_all(json.as_bytes()).await?;
    log::trace!("Writing line done.");
    log::trace!("Writing newline …");
    tx.write_u8(b'\n').await?;
    log::trace!("Writing newline done.");
    log::trace!("Flushing channel …");
    tx.flush().await?;
    log::trace!("Flushing channel done.");

    Ok(())
}
