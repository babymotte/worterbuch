use crate::server::common::CloneableWbApi;
use serde_json::json;
use std::time::Duration;
use tokio::time::{sleep, Instant};
use worterbuch_common::error::WorterbuchResult;

pub const SYSTEM_TOPIC_ROOT: &str = "$SYS";
pub const SYSTEM_TOPIC_CLIENTS: &str = "clients";
pub const SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSIONS: &str = "supportedProtocolVersions";
const VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn track_stats(wb: CloneableWbApi) -> WorterbuchResult<()> {
    let start = Instant::now();
    wb.set(format!("{SYSTEM_TOPIC_ROOT}/version"), json!(VERSION))
        .await?;
    loop {
        update_stats(&wb, start).await?;
        sleep(Duration::from_secs(1)).await;
    }
}

async fn update_stats(wb: &CloneableWbApi, start: Instant) -> WorterbuchResult<()> {
    update_uptime(wb, start.elapsed()).await?;
    update_message_count(wb).await?;
    Ok(())
}

async fn update_uptime(wb: &CloneableWbApi, uptime: Duration) -> WorterbuchResult<()> {
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/uptime"),
        json!(uptime.as_secs()),
    )
    .await
}

async fn update_message_count(wb: &CloneableWbApi) -> WorterbuchResult<()> {
    let len = wb.len().await?;
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/store/values/count"),
        json!(len),
    )
    .await
}
