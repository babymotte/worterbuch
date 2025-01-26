use super::config::Config;
use miette::{IntoDiagnostic, Result};
use tokio::net::UdpSocket;

pub async fn init_socket(config: &Config) -> Result<UdpSocket> {
    log::info!("Creating node socket …");

    let sock = UdpSocket::bind(format!("0.0.0.0:{}", config.orchestration_port))
        .await
        .into_diagnostic()?;

    // TODO configure socket

    Ok(sock)
}
