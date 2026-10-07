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

use std::{env, io};
use tracing::level_filters::LevelFilter;
use tracing::{debug, info};
use tracing_subscriber::{
    Layer, Registry, filter::Targets, fmt, layer::SubscriberExt, reload, util::SubscriberInitExt,
};
use worterbuch_common::error::ConfigResult;

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

fn read_targets(env: String) -> Option<Targets> {
    let targets_str = std::fs::read_to_string(&env)
        .ok()
        .and_then(|c| extract_targets_str(&c));

    let targets_str = targets_str.unwrap_or(env);

    eprintln!("Log targets {:?}", &targets_str);

    targets_str.parse().ok()
}

async fn read_targets_async(env: String) -> Option<Targets> {
    let targets_str = tokio::fs::read_to_string(&env)
        .await
        .ok()
        .and_then(|c| extract_targets_str(&c));

    let targets_str = targets_str.unwrap_or(env);

    debug!("Updated log targets {:?}", &targets_str);

    targets_str.parse().ok()
}

fn extract_targets_str(file_content: &str) -> Option<String> {
    file_content
        .lines()
        .find(|l| !l.starts_with("#"))
        .map(|l| l.trim().to_owned())
}

pub async fn reload_log_targets<S>(handle: &ReloadableTargets<S>) -> ConfigResult<()>
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
