/*
 *  Worterbuch statistics module
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

use crate::{INTERNAL_CLIENT_ID, server::CloneableWbApi};
use serde_json::json;
use std::time::Duration;
use tokio::{
    select,
    time::{Instant, interval},
};
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::debug;
#[cfg(not(feature = "commercial"))]
use worterbuch_common::SYSTEM_TOPIC_SOURCES;
use worterbuch_common::{
    SYSTEM_TOPIC_LICENSE, SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_VERSION, WbApi, error::WorterbuchResult,
    topic,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
#[cfg(not(feature = "commercial"))]
pub const LICENSE: &str = env!("CARGO_PKG_LICENSE");
#[cfg(feature = "commercial")]
pub const LICENSE: &str = "COMMERCIAL";
#[cfg(not(feature = "commercial"))]
pub const REPO: &str = env!("CARGO_PKG_REPOSITORY");

pub async fn track_stats(wb: CloneableWbApi, subsys: &mut SubsystemHandle) -> WorterbuchResult<()> {
    let start = Instant::now();

    wb.set(
        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_VERSION),
        json!(VERSION),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_LICENSE),
        json!(LICENSE),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    #[cfg(feature = "commercial")]
    wb.set(
        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_LICENSE, "data"),
        json!(wb.config().license),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    #[cfg(not(feature = "commercial"))]
    wb.set(
        topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SOURCES),
        json!(format!("{REPO}/releases/tag/v{VERSION}")),
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

    debug!("stats subsystem completed.");

    Ok(())
}

async fn update_stats(wb: &CloneableWbApi, start: Instant) -> WorterbuchResult<()> {
    update_uptime(wb, start.elapsed()).await?;
    update_message_count(wb).await?;
    #[cfg(feature = "jemalloc")]
    update_jemalloc_stats(wb).await.ok();
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
    let len = wb.entries().await?;
    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/store/values/count"),
        json!(len),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await
}

#[cfg(feature = "jemalloc")]
async fn update_jemalloc_stats(wb: &CloneableWbApi) -> miette::Result<()> {
    use miette::IntoDiagnostic;
    use tikv_jemalloc_ctl::{epoch, stats};

    // Advance the epoch to refresh stats
    epoch::advance().into_diagnostic()?;

    let allocated = stats::allocated::read().into_diagnostic()?;
    let resident = stats::resident::read().into_diagnostic()?;
    let active = stats::active::read().into_diagnostic()?;
    let mapped = stats::mapped::read().into_diagnostic()?;
    let metadata = stats::metadata::read().into_diagnostic()?;
    let retained = stats::retained::read().into_diagnostic()?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/raw/allocated"),
        json!(allocated),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/raw/resident"),
        json!(resident),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/raw/active"),
        json!(active),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/raw/mapped"),
        json!(mapped),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/raw/retained"),
        json!(retained),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/raw/metadata"),
        json!(metadata),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/formatted/allocated"),
        json!(format_bytes(allocated)),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/formatted/resident"),
        json!(format_bytes(resident)),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/formatted/active"),
        json!(format_bytes(active)),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/formatted/mapped"),
        json!(format_bytes(mapped)),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/formatted/retained"),
        json!(format_bytes(retained)),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    wb.set(
        format!("{SYSTEM_TOPIC_ROOT}/jemalloc/formatted/metadata"),
        json!(format_bytes(metadata)),
        INTERNAL_CLIENT_ID.to_owned(),
    )
    .await?;

    Ok(())
}

#[cfg(feature = "jemalloc")]
pub fn format_bytes(bytes: usize) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;

    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{:.0} {}", size, UNITS[unit]) // no decimals for plain bytes
    } else {
        format!("{:.1} {}", size, UNITS[unit]) // one decimal otherwise
    }
}
