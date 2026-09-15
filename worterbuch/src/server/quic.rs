/*
 *  Worterbuch server QUIC module
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

//! A worterbuch client speaking QUIC is expected to open exactly one bidirectional
//! stream per connection and use it exactly like a TCP or unix socket connection:
//! newline delimited JSON messages, in order, for the lifetime of the connection.
//!
//! Because QUIC deliberately does not guarantee any ordering between different
//! streams of the same connection (that is the whole point of stream
//! multiplexing), worterbuch's guarantee that requests are processed in the exact
//! order they were sent could not be upheld if the protocol allowed one stream per
//! request. A single stream per connection sidesteps that problem entirely: bytes
//! within one QUIC stream are ordered exactly like a TCP byte stream.
//!
//! Since the server is the party that speaks first (it sends a `Welcome` message
//! immediately upon connecting, before the client has sent anything), and QUIC
//! requires whoever calls `open_bi` to write to it before the peer's `accept_bi`
//! can succeed, it is the *server* that opens the single bidirectional stream, not
//! the client. A QUIC client implementation therefore has to call `accept_bi` to
//! receive it.

use super::common::protocol::Proto;
use crate::{auth::JwtClaims, print_quic_endpoint, server::common::CloneableWbApi, stats::VERSION};
use hashbrown::HashMap;
use miette::{Context, IntoDiagnostic, Result};
use quinn::{
    Connection, Endpoint, Incoming, RecvStream, SendStream,
    rustls::pki_types::{CertificateDer, PrivateKeyDer},
};
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, Lines},
    select,
    sync::mpsc,
};
use tosub::Subsystem;
use totils::while_select;
use tracing::{debug, error, info, trace, warn};
use worterbuch_common::{
    ClientId, Protocol, WbApi,
    protocol::v1::{ProtocolVersion, ServerInfo, ServerMessage, Welcome},
    write_line_and_flush,
};

enum SocketEvent {
    Disconnected(Option<ClientId>),
    Connected(Box<Incoming>),
    Suppressed,
    EndpointClosed,
    ShutdownRequested,
}

pub async fn start(
    worterbuch: CloneableWbApi,
    bind_addr: IpAddr,
    port: u16,
    cert_path: PathBuf,
    key_path: PathBuf,
    subsys: Subsystem,
) -> Result<()> {
    let server_config = load_server_config(&cert_path, &key_path)?;

    let mut endpoints = Vec::new();
    let mut last_error = None;
    for addr in bind_addrs(bind_addr, port) {
        match bind_endpoint(server_config.clone(), addr) {
            Ok(endpoint) => {
                info!("Serving QUIC endpoint at {addr}");
                endpoints.push(endpoint);
            }
            Err(e) => {
                warn!("Failed to bind QUIC endpoint to {addr}: {e}");
                last_error = Some(e);
            }
        }
    }
    if endpoints.is_empty() {
        return Err(
            last_error.unwrap_or_else(|| miette::miette!("failed to bind any QUIC endpoint"))
        );
    }

    bind_endpoints_and_run(worterbuch, endpoints, subsys).await
}

async fn bind_endpoints_and_run(
    worterbuch: CloneableWbApi,
    endpoints: Vec<Endpoint>,
    subsys: Subsystem,
) -> Result<()> {
    let config = worterbuch.config().to_owned();
    if config.print_endpoints {
        for endpoint in &endpoints {
            print_quic_endpoint(endpoint.local_addr().into_diagnostic()?)?;
        }
    }

    // Multiple UDP sockets (one per bound address, see `bind_addrs`) are
    // funneled into a single incoming-connection queue so the rest of this
    // function can treat "a QUIC endpoint" as one logical thing regardless of
    // how many address families it is actually listening on.
    let (incoming_tx, mut incoming_rx) = mpsc::channel(100);
    for (i, endpoint) in endpoints.iter().enumerate() {
        let endpoint = endpoint.clone();
        let incoming_tx = incoming_tx.clone();
        subsys.spawn(format!("quic-accept-{i}"), async move |s| {
            loop {
                select! {
                    incoming = endpoint.accept() => match incoming {
                        Some(incoming) => if incoming_tx.send(incoming).await.is_err() {
                            break;
                        },
                        None => break,
                    },
                    _ = s.shutdown_requested() => break,
                }
            }
            Ok::<(), miette::Error>(())
        });
    }
    drop(incoming_tx);

    let (conn_closed_tx, mut conn_closed_rx) = mpsc::channel(100);
    let mut waiting_for_free_connections = false;

    let mut clients = HashMap::new();
    loop {
        let evt = next_socket_event(
            &subsys,
            &mut conn_closed_rx,
            &mut incoming_rx,
            waiting_for_free_connections,
        )
        .await;

        match evt {
            SocketEvent::Disconnected(uuid) => {
                if let Some(id) = uuid {
                    clients.remove(&id);
                    while let Ok(id) = conn_closed_rx.try_recv() {
                        clients.remove(&id);
                    }
                    debug!("{} QUIC connection(s) open.", clients.len());
                    waiting_for_free_connections = false;
                } else {
                    break;
                }
            }
            SocketEvent::Connected(incoming) => {
                debug!("Trying to accept new client connection.");
                let id = ClientId::new_v4();
                debug!("{} QUIC connection(s) open.", clients.len());
                let worterbuch = worterbuch.named(format!("client/{id}"));
                let conn_closed_tx = conn_closed_tx.clone();

                let supported_client_protocol_versions =
                    config.supported_client_protocol_versions();

                let client = subsys.spawn(format!("client-{id}"), async move |s| {
                    select! {
                        s = accept_and_serve(&s, id, *incoming, worterbuch, supported_client_protocol_versions) => if let Err(e) = s {
                            error!("Connection to client {id} closed with error: {e}");
                        },
                        _ = s.shutdown_requested() => (),
                    }
                    conn_closed_tx.send(id).await.ok();
                    Ok::<(), miette::Error>(())
                });
                clients.insert(id, client);
                debug!("Ready to accept new connections.");
            }
            SocketEvent::Suppressed => {}
            SocketEvent::EndpointClosed => {
                debug!("QUIC endpoint closed, no more connections will be accepted.");
                break;
            }
            SocketEvent::ShutdownRequested => break,
        }
    }

    for (cid, subsys) in clients {
        subsys.request_local_shutdown();
        debug!("Waiting for connection to client {cid} to close …");
        subsys.join().await.ok();
    }
    debug!("All clients disconnected.");

    for endpoint in &endpoints {
        endpoint.close(0u32.into(), b"server shutting down");
    }
    for endpoint in &endpoints {
        endpoint.wait_idle().await;
    }

    debug!("quicserver subsystem completed.");

    Ok(())
}

async fn next_socket_event(
    subsys: &Subsystem,
    conn_closed_rx: &mut mpsc::Receiver<ClientId>,
    incoming_rx: &mut mpsc::Receiver<Incoming>,
    waiting_for_free_connections: bool,
) -> SocketEvent {
    select! {
        recv = conn_closed_rx.recv() => SocketEvent::Disconnected(recv),
        incoming = incoming_rx.recv() => if waiting_for_free_connections {
            // dropping `incoming` here implicitly refuses the connection attempt
            SocketEvent::Suppressed
        } else {
            match incoming {
                Some(incoming) => SocketEvent::Connected(Box::new(incoming)),
                None => SocketEvent::EndpointClosed,
            }
        },
        _ = subsys.shutdown_requested() => SocketEvent::ShutdownRequested,
    }
}

/// The addresses to bind a QUIC UDP socket to for the given configured
/// `bind_addr`. "Loopback" is expanded to *both* the IPv4 and IPv6 loopback
/// addresses, bound as two separate sockets: unlike the unspecified/wildcard
/// address, binding to one specific loopback address does not also cover the
/// other family for free. Defaulting to a single family instead would mean a
/// client that resolves "localhost" to the family we didn't bind hangs for
/// its entire connection timeout instead of just connecting, since UDP has no
/// equivalent of TCP's immediate `ECONNREFUSED` when nothing is listening.
///
/// Anything else - a specific non-loopback address, or the wildcard address -
/// is bound exactly as configured, with no such expansion: only "give me the
/// loopback" is ambiguous enough between address families to warrant it, and
/// silently listening on more than what was explicitly configured would be a
/// security surprise.
fn bind_addrs(bind_addr: IpAddr, port: u16) -> Vec<SocketAddr> {
    if bind_addr.is_loopback() {
        vec![
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port),
        ]
    } else {
        vec![SocketAddr::new(bind_addr, port)]
    }
}

async fn accept_and_serve(
    subsys: &Subsystem,
    client_id: ClientId,
    incoming: Incoming,
    worterbuch: CloneableWbApi,
    supported_protocol_versions: Box<[ProtocolVersion]>,
) -> Result<()> {
    let connection = match incoming.await {
        Ok(connection) => connection,
        Err(e) => {
            warn!("QUIC handshake for client {client_id} failed: {e}");
            return Ok(());
        }
    };

    serve(
        subsys,
        client_id,
        connection,
        worterbuch,
        supported_protocol_versions,
    )
    .await
}

async fn serve(
    subsys: &Subsystem,
    client_id: ClientId,
    connection: Connection,
    worterbuch: CloneableWbApi,
    supported_protocol_versions: Box<[ProtocolVersion]>,
) -> Result<()> {
    let remote_addr = connection.remote_address();

    info!("New client connected: {client_id} ({remote_addr})");

    match worterbuch
        .connected(client_id, Some(remote_addr), Protocol::QUIC)
        .await
    {
        Ok(ejected) => {
            debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

            if let Err(e) = serve_loop(
                subsys,
                client_id,
                remote_addr,
                worterbuch.named("serve-loop"),
                connection,
                supported_protocol_versions,
                ejected,
            )
            .await
            {
                error!("Error in serve loop: {e}");
            }
        }
        Err(e) => error!("Error while adding new client: {e}"),
    }

    info!("Client disconnected: {client_id} ({remote_addr})");

    worterbuch
        .disconnected(client_id, Protocol::QUIC, Some(remote_addr))
        .await?;

    Ok(())
}

struct ServeLoop {
    client_id: ClientId,
    remote_addr: SocketAddr,
    authorized: Option<JwtClaims>,
    quic_rx: Lines<BufReader<RecvStream>>,
    proto: Proto,
    ejected: mpsc::Receiver<()>,
}

async fn serve_loop(
    subsys: &Subsystem,
    client_id: ClientId,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    connection: Connection,
    supported_protocol_versions: Box<[ProtocolVersion]>,
    ejected: mpsc::Receiver<()>,
) -> Result<()> {
    let config = worterbuch.config().to_owned();
    let authorization_required = config.auth_token_key.is_some();
    let send_timeout = config.send_timeout;
    let authorized = None;

    let (quic_tx, quic_rx) = connection
        .open_bi()
        .await
        .into_diagnostic()
        .context("failed to open QUIC stream to client")?;

    let (quic_send_tx, quic_send_rx) = mpsc::channel(config.channel_buffer_size);
    subsys.spawn("forward_messages_to_socket", async move |s| {
        forward_messages_to_socket(s, quic_send_rx, quic_tx, client_id, send_timeout).await
    });

    let quic_rx = BufReader::new(quic_rx);
    let quic_rx = quic_rx.lines();

    quic_send_tx
        .send(ServerMessage::Welcome(Welcome {
            client_id,
            info: ServerInfo::new(
                VERSION.to_owned(),
                supported_protocol_versions,
                authorization_required,
            ),
        }))
        .await
        .into_diagnostic()?;

    let proto = Proto::new(
        client_id,
        quic_send_tx,
        authorization_required,
        config,
        worterbuch,
    );

    let serve_loop = ServeLoop {
        authorized,
        client_id,
        proto,
        remote_addr,
        quic_rx,
        ejected,
    };

    serve_loop.run().await
}

async fn forward_messages_to_socket(
    subsys: Subsystem,
    mut quic_send_rx: mpsc::Receiver<ServerMessage>,
    mut quic_tx: SendStream,
    client_id: ClientId,
    send_timeout: Option<Duration>,
) -> Result<()> {
    loop {
        select! {
            recv = quic_send_rx.recv() => if let Some(msg) = recv {
                if let Err(e) = write_line_and_flush(|| subsys.shutdown_requested(), &msg, &mut quic_tx, send_timeout, client_id).await {
                    error!("Error sending QUIC message '{msg:?}': {e}");
                    break;
                }
            } else {
                debug!("Message forwarding to client {client_id} stopped: channel closed.");
                break;
            },
            _ = subsys.shutdown_requested() => {
                debug!("Message forwarding to client {client_id} stopped: subsystem stopped.");
                break;
            },
        }
    }

    Ok(())
}

impl ServeLoop {
    async fn run(mut self) -> Result<()> {
        while_select! {
            biased;
            _ = self.ejected.recv() => {
                info!("Client {} was ejected.", self.client_id);
                break;
            },
            recv = self.quic_rx.next_line() => self.process_next_line(recv).await?,
        }
        Ok(())
    }

    async fn process_next_line(
        &mut self,
        next_line: Result<Option<String>, io::Error>,
    ) -> Result<ControlFlow<()>> {
        match next_line {
            Ok(Some(json)) => self.process_line(json).await,
            Ok(None) => self.done(),
            Err(e) => self.quic_error(e),
        }
    }

    async fn process_line(&mut self, json: String) -> Result<ControlFlow<()>> {
        trace!("Processing incoming message …");
        let msg_processed = self
            .proto
            .process_incoming_message(&json, &mut self.authorized)
            .await?;
        if !msg_processed {
            return Ok(ControlFlow::Break(()));
        }
        trace!("Processing incoming message done.");
        Ok(ControlFlow::Continue(()))
    }

    fn quic_error(&mut self, e: io::Error) -> std::result::Result<ControlFlow<()>, miette::Error> {
        warn!(
            "QUIC stream of client {} ({}) closed with error:, {}",
            self.client_id, self.remote_addr, e
        );
        Ok(ControlFlow::Break(()))
    }

    fn done(&self) -> std::result::Result<ControlFlow<()>, miette::Error> {
        debug!(
            "QUIC stream of client {} ({}) closed normally.",
            self.client_id, self.remote_addr
        );
        Ok(ControlFlow::Break(()))
    }
}

/// Binds the QUIC UDP socket and wraps it in an [`Endpoint`].
///
/// If `addr` is the unspecified IPv6 address (`::`), the socket is bound with
/// `IPV6_V6ONLY` explicitly disabled so it also accepts IPv4 traffic
/// (including IPv4-mapped addresses from dual-stack-aware peers). This
/// matters because UDP has no equivalent of TCP's immediate `ECONNREFUSED`
/// when nothing is listening: a client that resolves "localhost" to `::1`
/// first (as most systems do, per RFC 6724, regardless of `/etc/hosts`
/// ordering) would otherwise burn its entire connection timeout on a QUIC
/// handshake attempt that can never get a response, before falling back to
/// an address that actually works.
fn bind_endpoint(server_config: quinn::ServerConfig, addr: SocketAddr) -> Result<Endpoint> {
    let domain = if addr.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    let socket = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
        .into_diagnostic()
        .context("failed to create QUIC UDP socket")?;
    if addr.ip().is_unspecified() && domain == socket2::Domain::IPV6 {
        // Best-effort: platforms without dual-stack support (or where this
        // isn't permitted) still get a working, IPv6-only QUIC endpoint.
        socket.set_only_v6(false).ok();
    }
    socket
        .bind(&addr.into())
        .into_diagnostic()
        .with_context(|| format!("failed to bind QUIC UDP socket to {addr}"))?;
    socket
        .set_nonblocking(true)
        .into_diagnostic()
        .context("failed to configure QUIC UDP socket")?;

    let runtime = quinn::default_runtime()
        .ok_or_else(|| miette::miette!("no async runtime found for QUIC endpoint"))?;
    Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server_config),
        socket.into(),
        runtime,
    )
    .into_diagnostic()
    .context("failed to create QUIC endpoint")
}

/// QUIC mandates ALPN protocol negotiation (unlike TLS over TCP, where it is
/// optional): a handshake with no negotiated application protocol MUST be
/// rejected with a `no_application_protocol` alert (RFC 9001, Section 8.1). A
/// worterbuch QUIC client must offer this protocol name in its ALPN extension.
const ALPN_PROTOCOL: &[u8] = b"worterbuch";

fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<quinn::ServerConfig> {
    let cert_chain = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let provider = Arc::new(quinn::rustls::crypto::ring::default_provider());
    let mut crypto = quinn::rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])
        .into_diagnostic()
        .context("failed to select TLS 1.3 for QUIC")?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .into_diagnostic()
        .context("failed to build QUIC TLS server configuration")?;
    crypto.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    crypto.max_early_data_size = u32::MAX;

    let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
        .into_diagnostic()
        .context("failed to build QUIC crypto configuration")?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    // quinn's default max_idle_timeout is 30s and it never sends keep-alive
    // packets on its own, so a connection with no application traffic for
    // that long (e.g. an idle TUI session) would otherwise be silently
    // dropped. A periodic PING well under that timeout keeps it alive.
    let mut transport_config = quinn::TransportConfig::default();
    transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
    server_config.transport_config(Arc::new(transport_config));

    Ok(server_config)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .into_diagnostic()
        .with_context(|| format!("failed to open QUIC certificate file {}", path.display()))?;
    let mut reader = io::BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .into_diagnostic()
        .with_context(|| format!("failed to parse QUIC certificate file {}", path.display()))
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)
        .into_diagnostic()
        .with_context(|| format!("failed to open QUIC private key file {}", path.display()))?;
    let mut reader = io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .into_diagnostic()
        .with_context(|| format!("failed to parse QUIC private key file {}", path.display()))?
        .ok_or_else(|| miette::miette!("no private key found in {}", path.display()))
}
