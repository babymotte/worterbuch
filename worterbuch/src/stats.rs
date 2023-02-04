use crate::{config::Config, worterbuch::Worterbuch};
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

pub async fn track_stats(wb: Arc<RwLock<Worterbuch>>, config: Config) -> WorterbuchResult<()> {
    let start = Instant::now();
    let separator = config.separator;
    wb.write().await.set(
        format!("{SYSTEM_TOPIC_ROOT}{separator}version"),
        json!(VERSION),
    )?;
    loop {
        update_stats(&wb, start, &config).await?;
        sleep(Duration::from_secs(10)).await;
    }
}

async fn update_stats(
    wb: &Arc<RwLock<Worterbuch>>,
    start: Instant,
    config: &Config,
) -> WorterbuchResult<()> {
    let mut wb_write = wb.write().await;
    update_uptime(&mut wb_write, start.elapsed(), config)?;
    update_message_count(&mut wb_write, config)?;
    Ok(())
}

fn update_uptime(wb: &mut Worterbuch, uptime: Duration, config: &Config) -> WorterbuchResult<()> {
    let separator = config.separator;
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}{separator}uptime"),
        json!(uptime.as_secs()),
    )
}

fn update_message_count(wb: &mut Worterbuch, config: &Config) -> WorterbuchResult<()> {
    let separator = config.separator;
    let len = wb.len();
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}{separator}store{separator}values{separator}count"),
        json!(len),
    )
}
