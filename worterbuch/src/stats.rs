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

use crate::{INTERNAL_CLIENT_ID, server::common::CloneableWbApi};
use serde_json::json;
use std::{ops::ControlFlow, time::Duration};
use tokio::time::{Instant, interval};
use tosub::Subsystem;
use totils::while_select;
use tracing::debug;
#[cfg(not(feature = "commercial"))]
use worterbuch_common::protocol::v1::SYSTEM_TOPIC_SOURCES;
use worterbuch_common::{
    Protocol, WbApi,
    error::WorterbuchResult,
    protocol::v1::{
        ClientInfo, SYSTEM_TOPIC_COUNT, SYSTEM_TOPIC_LICENSE, SYSTEM_TOPIC_ROOT,
        SYSTEM_TOPIC_STORE, SYSTEM_TOPIC_UPTIME, SYSTEM_TOPIC_VALUES, SYSTEM_TOPIC_VERSION, Set,
    },
    topic,
};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
#[cfg(not(feature = "commercial"))]
pub const LICENSE: &str = env!("CARGO_PKG_LICENSE");
#[cfg(feature = "commercial")]
pub const LICENSE: &str = "COMMERCIAL";
#[cfg(not(feature = "commercial"))]
pub const REPO: &str = env!("CARGO_PKG_REPOSITORY");

fn client_info() -> ClientInfo {
    ClientInfo {
        client_id: INTERNAL_CLIENT_ID,
        protocol: Protocol::Local,
    }
}

pub async fn track_stats(wb: CloneableWbApi, subsys: Subsystem) -> WorterbuchResult<()> {
    let start = Instant::now();

    let req = Set {
        transaction_id: 0,
        key: topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_VERSION),
        value: json!(VERSION),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_LICENSE),
        value: json!(LICENSE),
    };
    wb.set(req, client_info()).await?;

    #[cfg(feature = "commercial")]
    {
        let req = Set {
            transaction_id: 0,
            key: topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_LICENSE, "data"),
            value: json!(wb.config().license),
        };
        wb.set(req, client_info()).await?;
    }

    #[cfg(not(feature = "commercial"))]
    {
        let req = Set {
            transaction_id: 0,
            key: topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_SOURCES),
            value: json!(format!("{REPO}/releases/tag/v{VERSION}")),
        };
        wb.set(req, client_info()).await?;
    }

    let mut interval = interval(Duration::from_secs(1));

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        _ = interval.tick() => update_stats(&wb, start).await,
    }

    debug!("stats subsystem completed.");

    Ok(())
}

async fn update_stats(wb: &CloneableWbApi, start: Instant) -> ControlFlow<()> {
    if update_uptime(wb, start.elapsed()).await.is_err() {
        return ControlFlow::Break(());
    }
    if update_message_count(wb).await.is_err() {
        return ControlFlow::Break(());
    }
    #[cfg(feature = "jemalloc")]
    if update_jemalloc_stats(wb).await.is_err() {
        return ControlFlow::Break(());
    }
    ControlFlow::Continue(())
}

async fn update_uptime(wb: &CloneableWbApi, uptime: Duration) -> WorterbuchResult<()> {
    let req = Set {
        transaction_id: 0,
        key: topic!(SYSTEM_TOPIC_ROOT, SYSTEM_TOPIC_UPTIME),
        value: json!(uptime.as_secs()),
    };
    wb.set(req, client_info()).await
}

async fn update_message_count(wb: &CloneableWbApi) -> WorterbuchResult<()> {
    let len = wb.entries().await?;
    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_STORE,
            SYSTEM_TOPIC_VALUES,
            SYSTEM_TOPIC_COUNT
        ),
        value: json!(len),
    };
    wb.set(req, client_info()).await
}

#[cfg(feature = "jemalloc")]
async fn update_jemalloc_stats(wb: &CloneableWbApi) -> miette::Result<()> {
    use miette::IntoDiagnostic;
    use tikv_jemalloc_ctl::{epoch, stats};
    use worterbuch_common::protocol::v1::{
        SYSTEM_TOPIC_FORMATTED, SYSTEM_TOPIC_JEMALLOC, SYSTEM_TOPIC_RAW,
    };

    // Advance the epoch to refresh stats
    epoch::advance().into_diagnostic()?;

    let allocated = stats::allocated::read().into_diagnostic()?;
    let resident = stats::resident::read().into_diagnostic()?;
    let active = stats::active::read().into_diagnostic()?;
    let mapped = stats::mapped::read().into_diagnostic()?;
    let metadata = stats::metadata::read().into_diagnostic()?;
    let retained = stats::retained::read().into_diagnostic()?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_RAW,
            "allocated"
        ),
        value: json!(allocated),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_RAW,
            "resident"
        ),
        value: json!(resident),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_RAW,
            "active"
        ),
        value: json!(active),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_RAW,
            "mapped"
        ),
        value: json!(mapped),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_RAW,
            "retained"
        ),
        value: json!(retained),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_RAW,
            "metadata"
        ),
        value: json!(metadata),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_FORMATTED,
            "allocated"
        ),
        value: json!(format_bytes(allocated)),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_FORMATTED,
            "resident"
        ),
        value: json!(format_bytes(resident)),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_FORMATTED,
            "active"
        ),
        value: json!(format_bytes(active)),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_FORMATTED,
            "mapped"
        ),
        value: json!(format_bytes(mapped)),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_FORMATTED,
            "retained"
        ),
        value: json!(format_bytes(retained)),
    };
    wb.set(req, client_info()).await?;

    let req = Set {
        transaction_id: 0,
        key: topic!(
            SYSTEM_TOPIC_ROOT,
            SYSTEM_TOPIC_JEMALLOC,
            SYSTEM_TOPIC_FORMATTED,
            "metadata"
        ),
        value: json!(format_bytes(metadata)),
    };
    wb.set(req, client_info()).await?;

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
