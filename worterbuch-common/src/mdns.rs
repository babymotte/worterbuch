/*
 *  Worterbuch server mDNS module
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

use hashbrown::HashMap;
use mdns_sd::ServiceInfo;
use miette::{Context, IntoDiagnostic};
use std::ops::ControlFlow;
use tokio::sync::mpsc;
use tosub::Subsystem;
use totils::while_select;
use tracing::{debug, info};

use crate::socket::list_public_ips;

struct MdnsRegistryAgent {
    subsys: Subsystem,
    rx: mpsc::Receiver<ServiceInfo>,
}

impl MdnsRegistryAgent {
    pub fn new(subsys: Subsystem, rx: mpsc::Receiver<ServiceInfo>) -> Self {
        Self { subsys, rx }
    }

    pub async fn run(&mut self) -> miette::Result<()> {
        let mdns = mdns_sd::ServiceDaemon::new()
            .into_diagnostic()
            .wrap_err("Failed to create daemon")?;
        let receiver = mdns
            .monitor()
            .into_diagnostic()
            .wrap_err("Failed to monitor daemon")?;

        // Spawn a task to handle mDNS events
        self.subsys
            .spawn("event-monitor", |s| monitor_mdns_events(s, receiver));

        while let Some(service_info) = self.rx.recv().await {
            // Register with the daemon, which publishes the service.
            mdns.register(service_info)
                .into_diagnostic()
                .wrap_err("Failed to register our service")?;
        }

        // Gracefully shutdown the daemon
        mdns.shutdown()
            .into_diagnostic()
            .wrap_err("Failed to shutdown daemon")?;

        info!("mDNS registry shut down.");

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MdnsRegistry {
    tx: mpsc::Sender<ServiceInfo>,
}

impl MdnsRegistry {
    pub fn start(subsys: &Subsystem) -> Self {
        let (tx, rx) = mpsc::channel(32);

        subsys.spawn("mdns-registry", move |s| async move {
            MdnsRegistryAgent::new(s, rx).run().await
        });

        Self { tx }
    }

    pub async fn publish_mdns_service(
        &self,
        service_type: impl Into<String>,
        instance_name: impl Into<String>,
        port: u16,
        properties: Option<HashMap<String, String>>,
    ) -> miette::Result<()> {
        let service_type = service_type.into();
        let instance_name = instance_name.into();

        info!(
            "Publishing '{}' service for '{}'",
            service_type, instance_name
        );

        // Create a service info.
        // Make sure that the service name: "mdns-sd-my-test" is not longer than the max length limit (15 by default).

        let ip = list_public_ips()?
            .into_iter()
            .map(|it| it.to_string())
            .reduce(|a, b| a + "," + &b)
            .unwrap_or_default();
        let hostname = gethostname::gethostname().to_string_lossy().into_owned();
        let host_name = format!("{}.local.", hostname);
        let properties = if let Some(properties) = properties {
            properties.into_iter().collect()
        } else {
            Vec::new()
        };

        let my_service = ServiceInfo::new(
            &service_type,
            &instance_name,
            &host_name,
            ip,
            port,
            &properties[..],
        )
        .into_diagnostic()
        .wrap_err("Failed to create service info")?;

        let _ = self.tx.send(my_service).await;

        Ok(())
    }
}

async fn monitor_mdns_events(
    subsys: Subsystem,
    receiver: mdns_sd::Receiver<mdns_sd::DaemonEvent>,
) -> miette::Result<()> {
    while_select! {
        _ = subsys.shutdown_requested() => break,
        recv = receiver.recv_async() => process_event(recv).await,
    }
    Ok(())
}

async fn process_event(event: Result<mdns_sd::DaemonEvent, mdns_sd::RecvError>) -> ControlFlow<()> {
    // TODO: Implement handling of mDNS events based on the received event.
    debug!("Received mDNS event: {:?}", event);
    ControlFlow::Continue(())
}
