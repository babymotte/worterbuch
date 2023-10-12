use std::io;

use crate::error::{ConnectionError, ConnectionResult};
use serde::Serialize;
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
    tx.write_all(json.as_bytes()).await?;
    tx.write_u8(b'\n').await?;
    tx.flush().await?;

    Ok(())
}
