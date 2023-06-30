use crate::worterbuch::Worterbuch;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::RwLock,
    time::{sleep, Instant},
};
use worterbuch_common::error::WorterbuchResult;

pub const SYSTEM_TOPIC_ROOT: &str = "$SYS";
pub const SYSTEM_TOPIC_CLIENTS: &str = "clients";
const VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn track_stats(wb: Arc<RwLock<Worterbuch>>) -> WorterbuchResult<()> {
    let start = Instant::now();
    wb.write()
        .await
        .set(format!("{SYSTEM_TOPIC_ROOT}/version"), json!(VERSION))?;
    loop {
        update_stats(&wb, start).await?;
        sleep(Duration::from_secs(10)).await;
    }
}

async fn update_stats(wb: &Arc<RwLock<Worterbuch>>, start: Instant) -> WorterbuchResult<()> {
    let mut wb_write = wb.write().await;
    update_uptime(&mut wb_write, start.elapsed())?;
    update_message_count(&mut wb_write)?;
    Ok(())
}

fn update_uptime(wb: &mut Worterbuch, uptime: Duration) -> WorterbuchResult<()> {
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/uptime"),
        json!(uptime.as_secs()),
    )
}

fn update_message_count(wb: &mut Worterbuch) -> WorterbuchResult<()> {
    let len = wb.len();
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/store/values/count"),
        json!(len),
    )
}
