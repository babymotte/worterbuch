use super::config::Config;
use miette::Result;
use tokio::{net::UdpSocket, select};
use tokio_graceful_shutdown::SubsystemHandle;

pub async fn lead(subsys: &SubsystemHandle, socket: &mut UdpSocket, config: &Config) -> Result<()> {
    // TODO
    // periodically send heartbeat request and evaluate responses
    // if fewer than quorum - 1 responses are received, assume there is a split brain and go back to follower mode
    log::info!("Starting worterbuch server in leader mode …");
    select! {
        // TODO
        // - start worterbuch server in leader mode
        // - open rest endpoint for clients to connect to to request leader socket addresses

        _ = subsys.on_shutdown_requested() => (),
    }

    Ok(())
}
