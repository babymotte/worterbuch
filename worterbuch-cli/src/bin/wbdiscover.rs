/*
 *  Worterbuch cli tool for discovering worterbuch servers via mDNS
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

use clap::Parser;
use core::panic;
use miette::{Context, IntoDiagnostic, Result};
use serde::Serialize;
use serde_json::json;
use std::{collections::BTreeMap, fmt, ops::ControlFlow, time::Duration};
use tokio::select;
use totils::while_select;
use tracing::{info, trace};
use worterbuch_client::{
    logging,
    mdns::{
        MDNS_SERVICE_TYPE_WB_HTTP, MDNS_SERVICE_TYPE_WB_QUIC, MDNS_SERVICE_TYPE_WB_TCP,
        MDNS_SERVICE_TYPE_WB_WS,
    },
};

#[derive(Debug, Parser)]
#[command(author, version, about = "Convert JSON into Wörterbuch key/value pairs.", long_about = None)]
struct Args {
    /// Output data in JSON format.
    #[arg(short, long)]
    json: bool,

    /// Discover TCP services.
    #[arg(long, conflicts_with_all = &["ws", "quic", "http"])]
    tcp: bool,

    /// Discover WebSocket services.
    #[arg(long, conflicts_with_all = &["tcp", "quic", "http"])]
    ws: bool,

    /// Discover QUIC services.
    #[arg(long, conflicts_with_all = &["tcp", "ws", "http"])]
    quic: bool,

    /// Discover HTTP services.
    #[arg(long, conflicts_with_all = &["tcp", "ws", "quic"])]
    http: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum Event<'a> {
    ServiceResolved(&'a mdns_sd::ResolvedService),
    ServiceRemoved { fullname: String, ty_domain: String },
}

impl<'a> fmt::Display for Event<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::ServiceResolved(service) => {
                writeln!(f, "Service resolved")?;
                write_property(f, "Name", &service.fullname)?;
                write_property(f, "Type", &service.ty_domain)?;
                write_property(
                    f,
                    "Subtype",
                    service.sub_ty_domain.as_deref().unwrap_or("-"),
                )?;
                write_property(f, "Host", &service.host)?;
                write_property(f, "Port", service.port)?;

                let mut addresses = service
                    .addresses
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                addresses.sort();
                write_list(f, "Addresses", addresses)?;

                let mut properties = service
                    .txt_properties
                    .iter()
                    .map(|p| match p.val() {
                        Some(val) => format!("{} = {}", p.key(), String::from_utf8_lossy(val)),
                        None => p.key().to_owned(),
                    })
                    .collect::<Vec<_>>();
                properties.sort();
                write_list(f, "Properties", properties)
            }
            Event::ServiceRemoved {
                fullname,
                ty_domain,
            } => {
                writeln!(f, "Service removed")?;
                write_property(f, "Name", fullname)?;
                write_property(f, "Type", ty_domain)
            }
        }
    }
}

const LABEL_WIDTH: usize = 12;

fn write_property(
    f: &mut fmt::Formatter<'_>,
    label: &str,
    value: impl fmt::Display,
) -> fmt::Result {
    writeln!(f, "  {:<LABEL_WIDTH$}{}", format!("{label}:"), value)
}

fn write_list(f: &mut fmt::Formatter<'_>, label: &str, items: Vec<String>) -> fmt::Result {
    let mut items = items.into_iter();
    let Some(first) = items.next() else {
        return write_property(f, label, "-");
    };
    write_property(f, label, first)?;
    for item in items {
        writeln!(f, "  {:<LABEL_WIDTH$}{}", "", item)?;
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> miette::Result<()> {
    let _ = dotenvy::dotenv();
    logging::init()?;
    let args: Args = Args::parse();

    tosub::build_default_root("wbdiscover")
        .start(move |s| run(s, args))
        .await?;

    Ok(())
}

async fn run(subsys: tosub::Subsystem, args: Args) -> miette::Result<()> {
    if args.tcp {
        discover_tcp(subsys, args.json).await?;
    } else if args.ws {
        discover_ws(subsys, args.json).await?;
    } else if args.quic {
        discover_quic(subsys, args.json).await?;
    } else if args.http {
        discover_http(subsys, args.json).await?;
    } else {
        discover_all(subsys, args.json).await?;
    }

    Ok(())
}

async fn discover_tcp(
    subsys: tosub::Subsystem,
    json: bool,
) -> std::prelude::v1::Result<(), miette::Error> {
    let mdns = mdns_sd::ServiceDaemon::new()
        .into_diagnostic()
        .wrap_err("Failed to create mDNS daemon")?;

    let tcp_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_TCP)
        .into_diagnostic()
        .wrap_err("Failed to browse for TCP mDNS service")?;

    let mut discovered_services = BTreeMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    while_select! {
        _ = subsys.shutdown_requested() => break,
        recv = tcp_browser.recv_async() => discovered(recv, json, &mut discovered_services),
        _ = wait(&mut interval) => print_new(&mut discovered_services, json),
    }

    subsys.request_global_shutdown_because("mDNS discovery channel closed");

    let shutdown = mdns
        .shutdown()
        .into_diagnostic()
        .wrap_err("Failed to shut down mDNS daemon")?;

    while let Ok(event) = shutdown.recv_async().await {
        info!("Received mDNS shutdown event: {:?}", event);
    }

    Ok(())
}

async fn discover_ws(
    subsys: tosub::Subsystem,
    json: bool,
) -> std::prelude::v1::Result<(), miette::Error> {
    let mdns = mdns_sd::ServiceDaemon::new()
        .into_diagnostic()
        .wrap_err("Failed to create mDNS daemon")?;

    let ws_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_WS)
        .into_diagnostic()
        .wrap_err("Failed to browse for WS mDNS service")?;

    let mut discovered_services = BTreeMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    while_select! {
        _ = subsys.shutdown_requested() => break,
        recv = ws_browser.recv_async() => discovered(recv,json, &mut discovered_services),
        _ = wait(&mut interval) => print_new(&mut discovered_services, json),
    }

    subsys.request_global_shutdown_because("mDNS discovery channel closed");

    let shutdown = mdns
        .shutdown()
        .into_diagnostic()
        .wrap_err("Failed to shut down mDNS daemon")?;

    while let Ok(event) = shutdown.recv_async().await {
        info!("Received mDNS shutdown event: {:?}", event);
    }

    Ok(())
}
async fn discover_quic(
    subsys: tosub::Subsystem,
    json: bool,
) -> std::prelude::v1::Result<(), miette::Error> {
    let mdns = mdns_sd::ServiceDaemon::new()
        .into_diagnostic()
        .wrap_err("Failed to create mDNS daemon")?;

    let quic_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_QUIC)
        .into_diagnostic()
        .wrap_err("Failed to browse for QUIC mDNS service")?;

    let mut discovered_services = BTreeMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    while_select! {
        _ = subsys.shutdown_requested() => break,
        recv = quic_browser.recv_async() => discovered(recv, json, &mut discovered_services),
        _ = wait(&mut interval) => print_new(&mut discovered_services, json),
    }

    subsys.request_global_shutdown_because("mDNS discovery channel closed");

    let shutdown = mdns
        .shutdown()
        .into_diagnostic()
        .wrap_err("Failed to shut down mDNS daemon")?;

    while let Ok(event) = shutdown.recv_async().await {
        info!("Received mDNS shutdown event: {:?}", event);
    }

    Ok(())
}
async fn discover_http(
    subsys: tosub::Subsystem,
    json: bool,
) -> std::prelude::v1::Result<(), miette::Error> {
    let mdns = mdns_sd::ServiceDaemon::new()
        .into_diagnostic()
        .wrap_err("Failed to create mDNS daemon")?;

    let http_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_HTTP)
        .into_diagnostic()
        .wrap_err("Failed to browse for HTTP mDNS service")?;

    let mut discovered_services = BTreeMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    while_select! {
        _ = subsys.shutdown_requested() => break,
        recv = http_browser.recv_async() => discovered(recv, json, &mut discovered_services),
        _ = wait(&mut interval) => print_new(&mut discovered_services, json),
    }

    subsys.request_global_shutdown_because("mDNS discovery channel closed");

    let shutdown = mdns
        .shutdown()
        .into_diagnostic()
        .wrap_err("Failed to shut down mDNS daemon")?;

    while let Ok(event) = shutdown.recv_async().await {
        info!("Received mDNS shutdown event: {:?}", event);
    }

    Ok(())
}

async fn discover_all(
    subsys: tosub::Subsystem,
    json: bool,
) -> std::prelude::v1::Result<(), miette::Error> {
    let mdns = mdns_sd::ServiceDaemon::new()
        .into_diagnostic()
        .wrap_err("Failed to create mDNS daemon")?;

    let ws_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_WS)
        .into_diagnostic()
        .wrap_err("Failed to browse for WebSocket mDNS service")?;

    let tcp_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_TCP)
        .into_diagnostic()
        .wrap_err("Failed to browse for TCP mDNS service")?;

    let quic_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_QUIC)
        .into_diagnostic()
        .wrap_err("Failed to browse for QUIC mDNS service")?;

    let http_browser = mdns
        .browse(MDNS_SERVICE_TYPE_WB_HTTP)
        .into_diagnostic()
        .wrap_err("Failed to browse for HTTP mDNS service")?;

    let mut discovered_services = BTreeMap::new();
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    while_select! {
        _ = subsys.shutdown_requested() => break,
        recv = ws_browser.recv_async() => discovered(recv, json, &mut discovered_services),
        recv = tcp_browser.recv_async() => discovered(recv, json, &mut discovered_services),
        recv = quic_browser.recv_async() => discovered(recv, json, &mut discovered_services),
        recv = http_browser.recv_async() => discovered(recv, json, &mut discovered_services),
        _ = wait(&mut interval) => print_new(&mut discovered_services, json),
    }

    subsys.request_global_shutdown_because("mDNS discovery channel closed");

    let shutdown = mdns
        .shutdown()
        .into_diagnostic()
        .wrap_err("Failed to shut down mDNS daemon")?;

    while let Ok(event) = shutdown.recv_async().await {
        info!("Received mDNS shutdown event: {:?}", event);
    }

    Ok(())
}

fn discovered(
    recv: Result<mdns_sd::ServiceEvent, mdns_sd::RecvError>,
    json: bool,
    discovered_services: &mut BTreeMap<String, (mdns_sd::ResolvedService, bool)>,
) -> ControlFlow<()> {
    let Ok(event) = recv else {
        return ControlFlow::Break(());
    };

    match event {
        mdns_sd::ServiceEvent::ServiceResolved(service) => {
            discovered_services.insert(service.fullname.clone(), (*service, false));
        }
        mdns_sd::ServiceEvent::ServiceRemoved(service_type, name) => {
            discovered_services.remove(&name);
            log_removed(service_type, name, json);
        }
        event => trace!(?event),
    }

    ControlFlow::Continue(())
}

fn print_new(
    discovered_services: &mut BTreeMap<String, (mdns_sd::ResolvedService, bool)>,
    json: bool,
) {
    for service in discovered_services.values_mut() {
        if !service.1 {
            log_resolved(&service.0, json);
            service.1 = true;
        }
    }
}

fn log_resolved(service: &mdns_sd::ResolvedService, json: bool) {
    let event = Event::ServiceResolved(service);
    if json {
        print_resolved_json(event);
    } else {
        log_resolved_info(event);
    }
}

fn log_removed(service_type: String, name: String, json: bool) {
    let event = Event::ServiceRemoved {
        fullname: name,
        ty_domain: service_type,
    };
    if json {
        print_removed_json(event);
    } else {
        log_removed_info(event);
    }
}

fn print_resolved_json(event: Event) {
    println!("{}", json!(event));
}

fn log_resolved_info(event: Event) {
    info!("{}", event);
}

fn print_removed_json(event: Event) {
    println!("{}", json!(event));
}

fn log_removed_info(event: Event) {
    info!("{}", event);
}

async fn wait(interval: &mut tokio::time::Interval) {
    let sleep = tokio::time::sleep(Duration::from_secs(3));
    select! {
        _ = interval.tick() => (),
        _ = sleep => {
            interval.reset();
        },
    }
}
