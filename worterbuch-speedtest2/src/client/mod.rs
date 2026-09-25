use miette::{Context, IntoDiagnostic, bail};
use std::{collections::BTreeSet, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader, BufWriter, Lines},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::mpsc,
    time::interval,
};
use tosub::Subsystem;
#[cfg(feature = "trace")]
use tracing::trace;
use tracing::{debug, info, warn};
use worterbuch_client::{
    Ack, AuthToken, AuthorizationRequest, ClientMessage, Key, ProtocolMajorVersion,
    ProtocolSwitchRequest, ServerMessage, Set, TransactionId, Value, Welcome, config::Config,
    write_line_and_flush,
};

pub trait Writer: AsyncWrite {}
pub trait Reader: AsyncBufReadExt {}

pub struct TestClient<W> {
    id: usize,
    next_tid: u64,
    set_commands: Vec<ClientMessage>,
    writer: BufWriter<W>,
    lines: mpsc::Receiver<String>,
    pending_acks: BTreeSet<TransactionId>,
}

impl<W: AsyncWrite + Unpin> TestClient<W> {
    pub async fn prepare_set(&mut self, key: Key, value: Value) {
        let transaction_id = self.next_tid();
        #[cfg(feature = "trace")]
        trace!(
            self.id,
            transaction_id,
            key,
            ?value,
            "preparing set command",
        );
        self.set_commands.push(ClientMessage::Set(Set {
            transaction_id,
            key,
            value,
        }))
    }

    fn next_tid(&mut self) -> u64 {
        let tid = self.next_tid;
        self.next_tid += 1;
        tid
    }

    pub async fn send_prepared_sets(&mut self, subsys: &Subsystem) -> miette::Result<()> {
        let set_commands = self.set_commands.drain(..).collect::<Vec<_>>();

        for msg in set_commands {
            let transaction_id = msg.transaction_id().expect("tid must be present");

            self.pending_acks.insert(transaction_id);

            select! {
                biased;
                _ = subsys.shutdown_requested() => break,
                res = write_line_and_flush(|| subsys.shutdown_requested(), msg, &mut self.writer, None) => {
                    res?;
                    #[cfg(feature = "trace")]
                    trace!(self.id, transaction_id, ?self.pending_acks, "sent set command");
                },
            }

            if let Ok(line) = self.lines.try_recv() {
                self.process_line(Some(line)).await?;
            }
        }
        Ok(())
    }

    pub async fn await_acks(&mut self, subsys: &Subsystem) -> miette::Result<()> {
        let mut interval = interval(Duration::from_secs(1));
        while !self.pending_acks.is_empty() {
            select! {
                biased;
                _ = subsys.shutdown_requested() => break,
                _ = interval.tick() => self.log_status(),
                recv = self.lines.recv() => self.process_line(recv).await?,
            }
        }
        Ok(())
    }

    async fn process_line(&mut self, line: Option<String>) -> miette::Result<()> {
        let Some(line) = line else {
            bail!("Failed to read line from server");
        };

        let Ok(msg) = serde_json::from_str::<ServerMessage>(&line) else {
            bail!("Received invalid data from server");
        };

        let ServerMessage::Ack(Ack { transaction_id }) = msg else {
            bail!("Received unexpected data from server");
        };

        if self.pending_acks.remove(&transaction_id) {
            #[cfg(feature = "trace")]
            trace!(self.id, transaction_id, ?self.pending_acks, "received ack");
        }

        Ok(())
    }

    fn log_status(&self) {
        info!(
            "Client {}: {:?} pending acks",
            self.id,
            self.pending_acks.len()
        );
    }
}

pub async fn create_tcp_client(
    subsys: &Subsystem,
    id: usize,
    client_config: &Config,
) -> miette::Result<TestClient<OwnedWriteHalf>> {
    let (mut writer, mut lines) = connect_tcp(id, &client_config).await?;

    let welcome = receive_welcome_message(id, &mut lines).await?;

    if welcome.info.authorization_required {
        if let Some(auth_token) = &client_config.auth_token {
            send_auth(id, subsys, auth_token.to_owned(), &mut writer, &mut lines).await?;
        } else {
            bail!("Authorization required but no auth token provided");
        }
    }

    let proto = find_proto(id, &welcome)?;

    switch_proto(id, subsys, proto, &mut writer, &mut lines).await?;

    let (lines_tx, lines_rx) = mpsc::channel(1024 * 1024);
    subsys.spawn("rx-reader", move |s| async move {
        loop {
            select! {
                biased;
                _ = s.shutdown_requested() => break,
                line = lines.next_line() => {
                    if let Ok(Some(line)) = line {
                        lines_tx.send(line).await.ok();
                    } else {
                        break;
                    }
                }
            }
        }
    });

    Ok(TestClient {
        id,
        next_tid: 1,
        set_commands: Vec::new(),
        writer,
        lines: lines_rx,
        pending_acks: Default::default(),
    })
}

async fn connect_tcp(
    id: usize,
    client_config: &Config,
) -> miette::Result<(BufWriter<OwnedWriteHalf>, Lines<BufReader<OwnedReadHalf>>)> {
    let mut stream = None;
    for addr in &client_config.servers {
        #[cfg(feature = "trace")]
        trace!(id, "Attempting to connect to server {} …", addr);
        match TcpStream::connect(addr).await {
            Ok(it) => {
                debug!(id, "Successfully connected to server {}", addr);
                stream = Some(it);
                break;
            }
            Err(e) => {
                warn!(id, "Failed to connect to server {}: {}", addr, e);
                continue;
            }
        };
    }
    let Some(stream) = stream else {
        bail!("Failed to connect to any server");
    };
    let (tcp_rx, tcp_tx) = stream.into_split();
    let writer = BufWriter::new(tcp_tx);
    let lines = BufReader::new(tcp_rx).lines();
    Ok((writer, lines))
}

async fn receive_welcome_message<R: AsyncRead + Unpin>(
    id: usize,
    lines: &mut Lines<BufReader<R>>,
) -> miette::Result<Welcome> {
    debug!(id, "Receiving welcome message …");

    let Some(line) = lines
        .next_line()
        .await
        .into_diagnostic()
        .wrap_err("Error reading welcome message from TCP stream")?
    else {
        bail!("TCP stream closed before welcome message was received");
    };
    #[cfg(feature = "trace")]
    trace!(id, line, "line read from tcp stream");

    let msg = serde_json::from_str::<ServerMessage>(&line)
        .into_diagnostic()
        .wrap_err("Error parsing server message")?;

    #[cfg(feature = "trace")]
    trace!(id, ?msg, "server message parsed");

    let ServerMessage::Welcome(welcome) = msg else {
        bail!("Expected welcome message, got {:?}", msg);
    };

    debug!(id, "Welcome message received.");

    Ok(welcome)
}

async fn send_auth<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    id: usize,
    subsys: &Subsystem,
    auth_token: AuthToken,
    writer: &mut BufWriter<W>,
    lines: &mut Lines<BufReader<R>>,
) -> miette::Result<()> {
    debug!(id, "Sending auth message …");
    let msg = ClientMessage::AuthorizationRequest(AuthorizationRequest { auth_token });
    #[cfg(feature = "trace")]
    trace!(id, ?msg, "sending auth request");
    write_line_and_flush(
        || subsys.shutdown_requested(),
        msg,
        writer,
        Some(Duration::from_secs(5)),
    )
    .await
    .wrap_err("error sending auth request")?;
    #[cfg(feature = "trace")]
    trace!(id, "auth request sent");

    #[cfg(feature = "trace")]
    trace!(id, "waiting for auth ack");
    let Some(line) = lines
        .next_line()
        .await
        .into_diagnostic()
        .wrap_err("Error reading auth ack from TCP stream")?
    else {
        bail!("TCP stream closed before auth ack was received");
    };
    #[cfg(feature = "trace")]
    trace!(id, line, "data from server received");

    let msg = serde_json::from_str::<ServerMessage>(&line)
        .into_diagnostic()
        .wrap_err("Error parsing server message")?;

    let ServerMessage::Ack(Ack { transaction_id }) = msg else {
        bail!("Expected auth ack, got {:?}", msg);
    };
    #[cfg(feature = "trace")]
    trace!(id, transaction_id, "ack received");

    if transaction_id != 0 {
        bail!(
            "Auth ack returned unexpected transaction ID: {}",
            transaction_id
        );
    }

    debug!(id, "Auth successful.");

    Ok(())
}

fn find_proto(id: usize, welcome: &Welcome) -> miette::Result<ProtocolMajorVersion> {
    debug!(id, ?welcome, "Finding matching protocol …");
    // TODO compare supported server versions with own supported version and match
    let proto_version = 2;
    debug!(id, "Matching protocol found.");
    Ok(proto_version)
}

async fn switch_proto<W: AsyncWrite + Unpin, R: AsyncRead + Unpin>(
    id: usize,
    subsys: &Subsystem,
    proto: ProtocolMajorVersion,
    writer: &mut BufWriter<W>,
    lines: &mut Lines<BufReader<R>>,
) -> miette::Result<()> {
    debug!(id, ?proto, "Switching protocol …");

    let msg = ClientMessage::ProtocolSwitchRequest(ProtocolSwitchRequest { version: proto });
    #[cfg(feature = "trace")]
    trace!(id, ?msg, "sending protocol switch request");
    write_line_and_flush(
        || subsys.shutdown_requested(),
        msg,
        writer,
        Some(Duration::from_secs(5)),
    )
    .await
    .wrap_err("error switching protocol")?;
    #[cfg(feature = "trace")]
    trace!(id, "protocol switch request sent");

    #[cfg(feature = "trace")]
    trace!(id, "waiting for ack");
    let Some(line) = lines
        .next_line()
        .await
        .into_diagnostic()
        .wrap_err("Error reading protocol switch ack from TCP stream")?
    else {
        bail!("TCP stream closed before protocol switch ack was received");
    };
    #[cfg(feature = "trace")]
    trace!(id, line, "data from server received");

    let msg = serde_json::from_str::<ServerMessage>(&line)
        .into_diagnostic()
        .wrap_err("Error parsing server message")?;

    let ServerMessage::Ack(Ack { transaction_id }) = msg else {
        bail!("Expected protocol switch ack, got {:?}", msg);
    };
    #[cfg(feature = "trace")]
    trace!(id, transaction_id, "ack received");

    if transaction_id != 0 {
        bail!(
            "Protocol switch ack returned unexpected transaction ID: {}",
            transaction_id
        );
    }

    debug!(id, "Protocol switched.");
    Ok(())
}
