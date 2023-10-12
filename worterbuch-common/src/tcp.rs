use crate::error::ConnectionResult;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

pub async fn write_line_and_flush(
    msg: impl Serialize,
    mut tx: impl AsyncWriteExt + Unpin,
) -> ConnectionResult<()> {
    let json = serde_json::to_string(&msg)?;
    tx.write(json.as_bytes()).await?;
    tx.write_u8(b'\n').await?;
    tx.flush().await?;

    Ok(())
}
