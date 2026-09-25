use std::io;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init() -> miette::Result<()> {
    let log_layer = console_log_layer();
    let subscriber = tracing_subscriber::registry().with(log_layer);
    subscriber.init();
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
