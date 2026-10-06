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

use std::io;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worterbuch_common::error::ConfigResult;

pub fn init() -> ConfigResult<()> {
    let log_layer = console_log_layer();
    let subscriber = tracing_subscriber::registry().with(log_layer);
    subscriber.init();
    info!("Telemetry disabled by feature flag.");
    Ok(())
}

pub fn console_log_layer<
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
>() -> impl Layer<S> {
    let color = supports_color::on(supports_color::Stream::Stderr)
        .map(|it| it.has_basic || it.has_256 || it.has_16m)
        .unwrap_or(false);

    let writer = io::stderr;

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("WORTERBUCH_LOG")
        .from_env_lossy();

    fmt::Layer::new()
        .with_ansi(color)
        .with_writer(writer)
        .with_filter(env_filter)
}
