mod backend;
mod controller;
mod tui;

use std::fs::OpenOptions;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// The TUI owns the terminal, so logs must not go to stdout/stderr. Logging is
/// therefore opt-in: set `WORTERBUCH_TUI_LOG` to a file path to enable it, and
/// use `RUST_LOG` to control verbosity.
fn init_logging() {
    let Ok(path) = std::env::var("WORTERBUCH_TUI_LOG_FILE") else {
        return;
    };
    let Ok(file) = OpenOptions::new().create(true).append(true).open(&path) else {
        return;
    };

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_ansi(false)
                .with_writer(file)
                .with_filter(EnvFilter::from_env("WORTERBUCH_LOG")),
        )
        .init();
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    dotenvy::dotenv().ok();

    init_logging();

    tosub::build_default_root("worterbuch-tui")
        .start(backend::start)
        .await?;

    Ok(())
}
