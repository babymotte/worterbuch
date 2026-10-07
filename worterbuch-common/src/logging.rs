/*
 *  Logging utilities for the Worterbuch server.
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

use crate::error::ConfigResult;
use std::time::Duration;
use std::{env, io};
use tosub::Subsystem;
use totils::while_select;
use tracing::level_filters::LevelFilter;
use tracing::{debug, info};
use tracing_subscriber::{
    Layer, Registry, filter::Targets, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt,
};

pub fn init() -> ConfigResult<reload::Handle<impl Layer<Registry>, Registry>> {
    let (log_layer, log_handle) = console_log_layer();
    let subscriber = tracing_subscriber::registry().with(log_layer);
    subscriber.init();
    info!("Telemetry disabled by feature flag.");
    Ok(log_handle)
}

pub type ReloadableTargets<S> = reload::Handle<Targets, S>;

pub fn console_log_layer<S>() -> (impl Layer<S>, ReloadableTargets<S>)
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let color = supports_color::on(supports_color::Stream::Stderr)
        .map(|it| it.has_basic || it.has_256 || it.has_16m)
        .unwrap_or(false);

    let writer = io::stderr;

    let targets = env::var("WORTERBUCH_LOG")
        .ok()
        .and_then(|t| read_targets(t))
        .unwrap_or_else(|| Targets::new().with_default(LevelFilter::INFO));

    let (reloadable_targets, targets_reload_handler) = reload::Layer::new(targets);

    let stderr_layer = fmt::Layer::new()
        .with_ansi(color)
        .with_writer(writer)
        .with_filter(reloadable_targets);

    (stderr_layer, targets_reload_handler)
}

pub async fn reload_log_targets_from_env<S>(handle: &ReloadableTargets<S>) -> ConfigResult<()>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let targets = env::var("WORTERBUCH_LOG").ok();

    let targets = if let Some(t) = targets {
        read_targets_async(t)
            .await
            .unwrap_or_else(|| Targets::new().with_default(LevelFilter::INFO))
    } else {
        Targets::new().with_default(LevelFilter::INFO)
    };

    handle.modify(|filter| *filter = targets)?;

    Ok(())
}

pub fn reload_log_targets<S>(
    handle: &ReloadableTargets<S>,
    targets: Option<String>,
) -> ConfigResult<()>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    let targets = read_targets_from_string(targets, "info")
        .unwrap_or_else(|| Targets::new().with_default(LevelFilter::INFO));
    handle.modify(|filter| *filter = targets)?;

    Ok(())
}

fn read_targets(env: String) -> Option<Targets> {
    read_targets_from_string(std::fs::read_to_string(&env).ok(), env)
}

async fn read_targets_async(env: String) -> Option<Targets> {
    read_targets_from_string(tokio::fs::read_to_string(&env).await.ok(), env)
}

fn read_targets_from_string(
    targets: Option<String>,
    default: impl Into<String>,
) -> Option<Targets> {
    let targets_str = targets.and_then(|c| extract_targets_str(&c));

    let targets_str = targets_str.unwrap_or_else(|| default.into());

    debug!("Updated log targets {:?}", &targets_str);

    targets_str.parse().ok()
}

fn extract_targets_str(targets_str: &str) -> Option<String> {
    targets_str
        .lines()
        .find(|l| !l.starts_with("#"))
        .map(|l| l.trim().to_owned())
}

pub fn start_log_targets_reload_loop(
    subsys: &Subsystem,
    log_reload_handle: ReloadableTargets<Registry>,
    reload_interval: Duration,
) {
    subsys.spawn("log-targets-scan", move |s| {
        scan_log_targets(s, log_reload_handle, reload_interval)
    });
}

async fn scan_log_targets(
    subsys: Subsystem,
    log_reload_handle: ReloadableTargets<Registry>,
    reload_interval: Duration,
) -> miette::Result<()> {
    while_select! {
        _ = subsys.shutdown_requested() => break,
        _ = tokio::time::sleep(reload_interval) => if let Err(e) = reload_log_targets_from_env(&log_reload_handle).await {
            eprintln!("Failed to reload log targets: {e}");
            break;
        },
    }
    Ok(())
}
