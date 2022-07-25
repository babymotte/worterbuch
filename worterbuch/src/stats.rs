use crate::worterbuch::Worterbuch;
use libworterbuch::error::WorterbuchResult;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::RwLock,
    time::{sleep, Instant},
};

const SYSTEM_TOPIC_ROOT: &str = "$SYS";

pub(crate) async fn track_stats(wb: Arc<RwLock<Worterbuch>>) -> WorterbuchResult<()> {
    let start = Instant::now();
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
        format!("{}", uptime.as_secs()),
    )
}

fn update_message_count(wb: &mut Worterbuch) -> WorterbuchResult<()> {
    let len = wb.len();
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/store/values/count"),
        len.to_string(),
    )
}
