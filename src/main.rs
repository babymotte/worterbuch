mod cluster_orchestrator;

use miette::Result;
use std::{io, time::Duration};
use tokio_graceful_shutdown::{SubsystemBuilder, Toplevel};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(io::stderr)
        .init();
    Toplevel::new(|s| async move {
        s.start(SubsystemBuilder::new(
            "cluster-orchestrator",
            cluster_orchestrator::run,
        ));
    })
    .catch_signals()
    .handle_shutdown_requests(Duration::from_secs(5))
    .await?;

    Ok(())
}
