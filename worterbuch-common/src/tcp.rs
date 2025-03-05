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
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Display, io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWriteExt, BufReader, Lines},
    time::timeout,
};
use tracing::{debug, error, trace};

pub async fn write_line_and_flush(
    msg: impl Serialize,
    mut tx: impl AsyncWriteExt + Unpin,
    send_timeout: Duration,
    remote: impl Display,
) -> ConnectionResult<()> {
    let mut json = serde_json::to_string(&msg)?;
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

    json.push('\n');
    let bytes = json.as_bytes();

    debug!("Sending message: {json}");
    trace!("Writing line …");
    for chunk in bytes.chunks(1024) {
        let mut written = 0;
        while written < chunk.len() {
            written += timeout(send_timeout, tx.write(&chunk[written..]))
                .await
                .map_err(|_| {
                    ConnectionError::Timeout(format!(
                        "timeout while sending tcp message to {remote}"
                    ))
                })??;
        }
    }
    trace!("Writing line done.");
    trace!("Flushing channel …");
    tx.flush().await?;
    trace!("Flushing channel done.");

    Ok(())
}

pub async fn receive_msg<T: DeserializeOwned, R: AsyncRead + Unpin>(
    rx: &mut Lines<BufReader<R>>,
) -> ConnectionResult<Option<T>> {
    let read = rx.next_line().await;
    match read {
        Ok(None) => Ok(None),
        Ok(Some(json)) => {
            debug!("Received message: {json}");
            let sm = serde_json::from_str(&json);
            if let Err(e) = &sm {
                error!("Error deserializing message '{json}': {e}")
            }
            Ok(sm?)
        }
        Err(e) => Err(e.into()),
    }
}
