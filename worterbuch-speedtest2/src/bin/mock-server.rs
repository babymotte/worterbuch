mod logging;

use miette::{Context, IntoDiagnostic};
use std::{
    io,
    net::{IpAddr, Ipv4Addr},
    ops::ControlFlow,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::TcpListener,
};
use tosub::Subsystem;
use totils::{CancelOn, while_select};
#[cfg(feature = "trace")]
use tracing::trace;
use tracing::{error, info};
use uuid::Uuid;
use worterbuch_client::{
    Ack, ClientMessage, ProtocolVersion, ServerInfo, ServerMessage, Welcome,
    socket::{TcpSocketConfig, create_tcp_server_socket},
};

#[tokio::main]
async fn main() -> miette::Result<()> {
    dotenvy::dotenv().ok();
    logging::init()?;

    tosub::build_default_root("worterbuch-speedtest")
        .with_timeout(Duration::from_secs(10))
        .start(run)
        .await?;

    Ok(())
}

async fn run(subsys: Subsystem) -> miette::Result<()> {
    subsys.spawn("tcp-listener", tcp_listener);

    subsys.shutdown_requested().await;

    Ok(())
}

async fn tcp_listener(subsys: Subsystem) -> miette::Result<()> {
    let socket = create_tcp_server_socket(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        9091,
        TcpSocketConfig::default(),
    )?;

    let socket = TcpListener::from_std(socket)
        .into_diagnostic()
        .wrap_err("Could not create async socket from regular socket")?;

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = socket.accept() => serve_client(&subsys, recv).await?,
    }

    Ok(())
}

async fn serve_client(
    subsys: &Subsystem,
    recv: io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>,
) -> miette::Result<ControlFlow<()>> {
    let (tcp_stream, remote_addr) = match recv {
        Ok(it) => it,
        Err(e) => {
            error!("Error while accepting client connection: {e}");
            return Ok(ControlFlow::Break(()));
        }
    };

    subsys.spawn(remote_addr.to_string(), move |s| {
        serve(s, tcp_stream, remote_addr)
    });

    Ok(ControlFlow::Continue(()))
}

async fn serve(
    subsys: Subsystem,
    tcp_stream: tokio::net::TcpStream,
    remote_addr: std::net::SocketAddr,
) -> miette::Result<()> {
    info!("Accepted connection from {remote_addr}");

    let (client_read, client_write) = tcp_stream.into_split();
    let mut client_read = BufReader::new(client_read).lines();
    let mut client_write = BufWriter::new(client_write);

    let welcome = ServerMessage::Welcome(Welcome {
        client_id: Uuid::new_v4(),
        info: ServerInfo::new(
            "0.1.0".into(),
            Box::new([ProtocolVersion(1, 1), ProtocolVersion(2, 0)]),
            false,
        ),
    });

    #[cfg(feature = "trace")]
    trace!(?welcome, "sending welcome message to client");

    let Some(res) = send_server_message(welcome, &mut client_write)
        .or_cancel_on(subsys.shutdown_requested())
        .await
    else {
        return Ok(());
    };
    res?;

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        line = client_read.next_line() => respond(&subsys, line, &mut client_write).await?,
    }

    info!("Closing connection from {remote_addr}");

    Ok(())
}

async fn respond(
    subsys: &Subsystem,
    line: io::Result<Option<String>>,
    client_write: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
) -> miette::Result<ControlFlow<()>> {
    let line = match line {
        Ok(Some(line)) => {
            #[cfg(feature = "trace")]
            trace!(line, "received line");
            line
        }
        Ok(None) => {
            #[cfg(feature = "trace")]
            trace!("client closed the connection");
            return Ok(ControlFlow::Break(()));
        }
        Err(e) => {
            error!("Error while reading line from client: {e}");
            return Ok(ControlFlow::Break(()));
        }
    };

    let msg: ClientMessage = match serde_json::from_str(&line) {
        Ok(msg) => msg,
        Err(e) => {
            error!("Error while parsing JSON from client: {e}");
            return Ok(ControlFlow::Break(()));
        }
    };

    // this is just a dumb mock server, it does not do anything meaningful with the messages, it just always responds with an ack

    let transaction_id = msg.transaction_id().unwrap_or(0);
    #[cfg(feature = "trace")]
    trace!(transaction_id, "sending Ack");
    let ack = ServerMessage::Ack(Ack { transaction_id });
    let Some(res) = send_server_message(ack, client_write)
        .or_cancel_on(subsys.shutdown_requested())
        .await
    else {
        return Ok(ControlFlow::Break(()));
    };
    res?;

    Ok(ControlFlow::Continue(()))
}

async fn send_server_message(
    msg: ServerMessage,
    client_write: &mut BufWriter<tokio::net::tcp::OwnedWriteHalf>,
) -> miette::Result<()> {
    let json = serde_json::to_string(&msg).expect("server messages must be serializeable");
    client_write
        .write_all(json.as_bytes())
        .await
        .into_diagnostic()
        .wrap_err("failed to write server message to TCP stream")?;
    client_write
        .write_all(b"\n")
        .await
        .into_diagnostic()
        .wrap_err("failed to write newline to TCP stream")?;
    client_write
        .flush()
        .await
        .into_diagnostic()
        .wrap_err("failed to flush TCP stream")?;
    Ok(())
}
