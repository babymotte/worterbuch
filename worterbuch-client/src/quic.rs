/*
 *  Worterbuch client QUIC module
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

use quinn::{
    Connection, RecvStream, SendStream,
    rustls::{
        self, DigitallySignedStruct, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        pki_types::{CertificateDer, ServerName, UnixTime},
    },
};
use rustls_platform_verifier::BuilderVerifierExt;
use std::{io, sync::Arc, time::Duration};
use tokio::{
    io::{BufReader, Lines},
    spawn,
    sync::{mpsc, oneshot},
};
use tracing::error;
use worterbuch_common::{
    error::{ConnectionError, ConnectionResult},
    protocol::v1::{ClientMessage, ServerMessage},
    write_line_and_flush,
};

use crate::{CancellationToken, config::Config};

const SERVER_ID: &str = "worterbuch server";

/// Must match `ALPN_PROTOCOL` in worterbuch's server, see
/// `worterbuch/src/server/quic.rs`.
const ALPN_PROTOCOL: &[u8] = b"worterbuch";

pub struct QuicClientSocket {
    tx: mpsc::Sender<ClientMessage>,
    rx: Lines<BufReader<RecvStream>>,
    closed: oneshot::Receiver<()>,
    // kept alive for as long as the socket is in use; streams hold their own
    // reference to the connection, but keeping this around too avoids any
    // ambiguity about when the connection is allowed to close
    _connection: Connection,
}

impl QuicClientSocket {
    pub(crate) async fn new(
        cancellation_token: CancellationToken,
        connection: Connection,
        tx: SendStream,
        rx: Lines<BufReader<RecvStream>>,
        send_timeout: Option<Duration>,
        buffer_size: usize,
    ) -> Self {
        let (send_tx, send_rx) = mpsc::channel(buffer_size);
        let (closed_tx, closed_rx) = oneshot::channel();
        spawn(forward_quic_messages(
            cancellation_token,
            tx,
            send_rx,
            send_timeout,
            closed_tx,
        ));
        Self {
            tx: send_tx,
            rx,
            closed: closed_rx,
            _connection: connection,
        }
    }

    pub async fn send_msg(&self, msg: ClientMessage, wait: bool) -> ConnectionResult<()> {
        if wait {
            self.tx.send(msg).await?;
        } else {
            self.tx.try_send(msg)?;
        }

        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        worterbuch_common::receive_msg(&mut self.rx, None).await
    }

    pub async fn close(self) -> ConnectionResult<()> {
        drop(self.tx);
        drop(self.rx);
        self.closed.await.ok();
        Ok(())
    }
}

async fn forward_quic_messages(
    cancellation_token: CancellationToken,
    mut tx: SendStream,
    mut send_rx: mpsc::Receiver<ClientMessage>,
    timeout: Option<Duration>,
    closed_tx: oneshot::Sender<()>,
) {
    while let Some(msg) = send_rx.recv().await {
        if let Err(e) = write_line_and_flush(
            || cancellation_token.clone().cancelled_owned(),
            msg,
            &mut tx,
            timeout,
            SERVER_ID,
        )
        .await
        {
            error!("Error sending QUIC message: {e}");
            break;
        }
    }

    tx.finish().ok();
    drop(tx);

    closed_tx.send(()).ok();
}

/// Builds the QUIC/TLS client configuration according to `config`:
/// - `config.quic_insecure`: accept any server certificate without verification
///   (only ever appropriate for local testing against a self-signed cert)
/// - `config.quic_ca_cert`: verify the server certificate against this single
///   CA/self-signed certificate instead of the platform's trust store
/// - otherwise: verify against the operating system's native trust store, same
///   as any other TLS client
pub(crate) fn build_client_config(config: &Config) -> ConnectionResult<quinn::ClientConfig> {
    if config.quic_insecure {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut crypto = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification(provider)))
            .with_no_client_auth();
        crypto.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];

        let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?;
        return Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)));
    }

    if let Some(ca_cert) = &config.quic_ca_cert {
        let file = std::fs::File::open(ca_cert)?;
        let mut reader = io::BufReader::new(file);
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut reader) {
            let cert = cert?;
            roots
                .add(cert)
                .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?;
        }

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut crypto = rustls::ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        crypto.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];

        let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?;
        return Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)));
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?
        .with_platform_verifier()
        .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?
        .with_no_client_auth();
    crypto.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];

    let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
        .map_err(|e| ConnectionError::IoError(Box::new(io::Error::other(e))))?;
    Ok(quinn::ClientConfig::new(Arc::new(quic_crypto)))
}

/// Accepts any server certificate without verification. Only appropriate for
/// local testing against a self-signed certificate (`quic_insecure` config
/// option) - never use this to connect to a server over an untrusted network.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
