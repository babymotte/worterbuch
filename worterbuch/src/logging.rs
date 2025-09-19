use std::io;
use supports_color::Stream;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use worterbuch_common::error::ConfigResult;

pub fn init() -> ConfigResult<()> {
    let subscriber = tracing_subscriber::registry().with(
        fmt::Layer::new()
            .with_ansi(supports_color::on(Stream::Stderr).is_some())
            .with_writer(io::stderr)
            .with_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .with_env_var("WORTERBUCH_LOG")
                    .from_env_lossy(),
            ),
    );

    subscriber.init();
    info!("Telemetry disabled by feature flag.");

    Ok(())
}
