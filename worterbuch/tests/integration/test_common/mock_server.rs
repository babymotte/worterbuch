/*
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
use miette::{Context, IntoDiagnostic, bail, ensure};
use serde_json::json;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader, BufWriter, Lines},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::oneshot,
};
use tosub::Subsystem;
use totils::while_select;
use tracing::{debug, error, info};
use worterbuch::{
    cluster::protocol::{
        ClientWriteCommand, ClusterStateChange, Connected, Disconnected, Handshake, LeaderMessage,
        LeaderWelcome, ProxyHandshake, ProxyMessage, Request, StateSync,
    },
    worterbuch_version,
};
use worterbuch_client::ClientInfo;
use worterbuch_common::{
    ClientId,
    protocol::v1::{
        Ack, ClientMessage, Key, Method, ServerMessage, Set, Trace, TransactionId, Value,
    },
    socket::{TcpSocketConfig, create_tcp_server_socket},
    write_line_and_flush,
};

const SEND_TIMEOUT: Duration = Duration::from_secs(5);

pub fn start(subsys: &Subsystem, sync_port: u16, started_tx: oneshot::Sender<()>) {
    subsys.spawn("mock-server", move |s| run(s, sync_port, started_tx));
}

async fn run(
    subsys: Subsystem,
    sync_port: u16,
    started_tx: oneshot::Sender<()>,
) -> miette::Result<()> {
    let tcp_listener = TcpListener::from_std(
        create_tcp_server_socket(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            sync_port,
            TcpSocketConfig::default(),
        )
        .unwrap(),
    )
    .unwrap();

    info!("Mock server started");
    started_tx.send(()).unwrap();

    while_select! {
        _ = subsys.shutdown_requested() => break,
        client = tcp_listener.accept() => accepted(&subsys, client).await.unwrap(),
    }

    info!("Mock server stopped");
    Ok(())
}

async fn accepted(
    subsys: &Subsystem,
    client: io::Result<(TcpStream, SocketAddr)>,
) -> miette::Result<()> {
    let (socket, addr) = client.into_diagnostic()?;

    info!("Accepted connection from {}", addr);

    subsys.spawn(format!("client/{addr}"), move |s| {
        serve_proxy(s, socket, addr)
    });

    Ok(())
}

async fn serve_proxy(subsys: Subsystem, socket: TcpStream, addr: SocketAddr) -> miette::Result<()> {
    info!("Serving proxy {}", addr);

    let (rx, tx) = socket.into_split();
    let mut tx = BufWriter::new(tx);
    let mut rx = BufReader::new(rx).lines();

    if let Err(e) = perform_handshake(&subsys, &mut rx, &mut tx).await {
        error!("Handshake with proxy {addr} failed: {e:?}");
        return Ok(());
    }

    info!("Handshake with proxy {addr} complete");

    let mut state = HashMap::new();

    if let Err(e) = while_select! {
        _ = subsys.shutdown_requested() => {
            break Err(miette::miette!("shutdown requested"));
        },
        line = rx.next_line() => process_line(&subsys, line, &mut tx, &mut state).await,
    } {
        error!("Client connection closed: {e}");
    }

    Ok(())
}

async fn perform_handshake(
    subsys: &Subsystem,
    rx: &mut Lines<BufReader<OwnedReadHalf>>,
    tx: &mut BufWriter<OwnedWriteHalf>,
) -> miette::Result<ProxyHandshake> {
    let welcome = LeaderMessage::Welcome(LeaderWelcome {
        authentication_required: false,
        version: worterbuch_version(),
    });
    send(subsys, welcome, tx).await?;

    let Some(line) = rx
        .next_line()
        .await
        .into_diagnostic()
        .wrap_err("error receiving handshake")?
    else {
        bail!("connection closed before handshake");
    };

    info!("Received handshake: {}", line);

    let ProxyMessage::Handshake(handshake) = serde_json::from_str(&line)
        .into_diagnostic()
        .wrap_err("could not parse proxy message")?
    else {
        bail!("expected handshake message, but got: {line}");
    };

    let Handshake::Proxy(handshake) = handshake else {
        bail!("expected proxy handshake, but got follower handshake");
    };

    ensure!(
        handshake.version == worterbuch_version(),
        "version mismatch: expected {}, got {}",
        worterbuch_version(),
        handshake.version
    );

    // TODO make the initial state configurable
    let state: StateSync = serde_json::from_value(json!({ "store": {} })).into_diagnostic()?;
    send(subsys, LeaderMessage::Init(state), tx).await?;

    Ok(handshake)
}

async fn process_line(
    subsys: &Subsystem,
    line: io::Result<Option<String>>,
    tx: &mut BufWriter<OwnedWriteHalf>,
    state: &mut HashMap<Key, Value>,
) -> miette::Result<()> {
    let Some(line) = line.into_diagnostic().wrap_err("tcp socket closed")? else {
        bail!("tcp connection closed");
    };

    debug!("Received line: {}", line);

    let msg: ProxyMessage = serde_json::from_str(&line)
        .into_diagnostic()
        .wrap_err("could not parse proxy message")?;

    match msg {
        ProxyMessage::Handshake(_) => {
            bail!("received handshake after initial handshake");
        }
        ProxyMessage::Connected(Connected { client_info }) => {
            info!("Client {} connected", client_info.client_id);
        }
        ProxyMessage::Disconnected(Disconnected { client_info, .. }) => {
            info!("Client {} disconnected", client_info.client_id);
        }
        ProxyMessage::Request(Request { msg, client_info }) => {
            let responses = process_request(msg, client_info, state)?;
            for response in responses {
                send(subsys, response, tx).await?;
            }
        }
    }

    Ok(())
}

fn process_request(
    msg: ClientMessage,
    client_info: ClientInfo,
    store: &mut HashMap<Key, Value>,
) -> miette::Result<Vec<LeaderMessage>> {
    let response = match msg {
        ClientMessage::Set(Set {
            transaction_id,
            key,
            value,
        }) => {
            store.insert(key.clone(), value.clone());
            vec![
                state_change(
                    transaction_id,
                    client_info.clone(),
                    ClientWriteCommand::Set(key, value),
                ),
                ack(transaction_id, client_info.client_id),
            ]
        }
        ClientMessage::ProtocolSwitchRequest(_) => vec![ack(0, client_info.client_id)],
        _ => {
            bail!("unexpected client message");
        }
    };

    Ok(response)
}

fn state_change(
    transaction_id: TransactionId,
    client_info: ClientInfo,
    command: ClientWriteCommand,
) -> LeaderMessage {
    let method = Method::from(&command);
    LeaderMessage::Mut(ClusterStateChange {
        client_id: client_info.client_id,
        command,
        trace: Trace::ClientRequest {
            client_info,
            transaction_id,
            method,
        },
    })
}

fn ack(transaction_id: TransactionId, client_id: ClientId) -> LeaderMessage {
    LeaderMessage::ClientResponse(client_id, ServerMessage::Ack(Ack { transaction_id }))
}

async fn send(
    subsys: &Subsystem,
    msg: LeaderMessage,
    tx: &mut BufWriter<OwnedWriteHalf>,
) -> miette::Result<()> {
    write_line_and_flush(|| subsys.shutdown_requested(), msg, tx, SEND_TIMEOUT)
        .await
        .into_diagnostic()
        .wrap_err("could not send message to proxy")
}
