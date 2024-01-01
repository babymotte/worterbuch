use crate::{server::common::CloneableWbApi, INTERNAL_CLIENT_ID};
use serde_json::json;
use std::time::Duration;
use tokio::{
    select,
    time::{interval, Instant},
};
use tokio_graceful_shutdown::SubsystemHandle;
use worterbuch_common::{error::WorterbuchResult, topic};

pub const SYSTEM_TOPIC_ROOT: &str = "$SYS";
pub const SYSTEM_TOPIC_CLIENTS: &str = "clients";
pub const SYSTEM_TOPIC_VERSION: &str = "version";
pub const SYSTEM_TOPIC_SUBSCRIPTIONS: &str = "subscriptions";
pub const SYSTEM_TOPIC_CLIENTS_PROTOCOL: &str = "protocol";
pub const SYSTEM_TOPIC_CLIENTS_ADDRESS: &str = "address";
pub const SYSTEM_TOPIC_LAST_WILL: &str = "lastWill";
pub const SYSTEM_TOPIC_GRAVE_GOODS: &str = "graveGoods";
pub const SYSTEM_TOPIC_SUPPORTED_PROTOCOL_VERSION: &str = "protocolVersion";
const VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn track_stats(wb: CloneableWbApi, subsys: SubsystemHandle) -> WorterbuchResult<()> {
    let start = Instant::now();
    wb.set(
        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_VERSION),
        json!(VERSION),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    let mut interval = interval(Duration::from_secs(1));

    loop {
        select! {
            _ = interval.tick() => update_stats(&wb, start).await?,
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    Ok(())
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
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await
}

async fn update_message_count(wb: &CloneableWbApi) -> WorterbuchResult<()> {
    let len = wb.len().await?;
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/store/values/count"),
        json!(len),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await
}
