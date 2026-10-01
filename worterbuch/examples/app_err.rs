use miette::{Context, IntoDiagnostic};
use std::io;
use tosub::Subsystem;
use worterbuch::error::{IntoWbAppResult, WorterbuchAppResult, WrappedResult};

#[tokio::main]
async fn main() -> miette::Result<()> {
    tosub::build_default_root("worterbuch").start(run).await?;
    Ok(())
}

async fn run(s: Subsystem) -> miette::Result<()> {
    s.spawn("wb-subsys", |s| async move {
        mock_wb_app(s)
            .await
            .into_diagnostic()
            .wrap_err("failed to start worterbuch")
            .wrap_err("did some crazy shit")
    });

    s.shutdown_requested().await;

    Ok(())
}

async fn mock_wb_app(s: Subsystem) -> WorterbuchAppResult<()> {
    s.spawn::<()>("wb-subsys", |_| async move {
        Err(io::Error::new(io::ErrorKind::Other, "oopsies"))
            .into_wb_app_result()
            .wrap("something went wrong")
    });

    s.shutdown_requested().await;

    Ok(())
}
