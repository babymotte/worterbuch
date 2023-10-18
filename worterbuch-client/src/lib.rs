pub mod buffer;
pub mod config;
pub mod error;
pub mod tcp;
pub mod ws;

use crate::config::Config;
use buffer::SendBuffer;
use error::SubscriptionError;
use futures_util::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{self as json};
use std::{
    collections::HashMap,
    future::Future,
    io,
    ops::ControlFlow,
    time::{Duration, Instant},
};
use tcp::TcpClientSocket;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    select, spawn,
    sync::{mpsc, oneshot},
    time::{interval, sleep, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use ws::WsClientSocket;

pub use worterbuch_common::*;
pub use worterbuch_common::{
    self,
    error::{ConnectionError, ConnectionResult},
    Ack, ClientMessage as CM, Delete, Err, Get, GraveGoods, HandshakeRequest, Key, KeyValuePairs,
    LastWill, LsState, PState, PStateEvent, ProtocolVersion, RegularKeySegment,
    ServerMessage as SM, Set, State, StateEvent, TransactionId,
};

#[derive(Debug)]
pub(crate) enum Command {
    Set(Key, Value, oneshot::Sender<TransactionId>),
    Publish(Key, Value, oneshot::Sender<TransactionId>),
    Get(Key, oneshot::Sender<(Option<Value>, TransactionId)>),
    GetAsync(Key, oneshot::Sender<TransactionId>),
    PGet(Key, oneshot::Sender<(KeyValuePairs, TransactionId)>),
    PGetAsync(Key, oneshot::Sender<TransactionId>),
    Delete(Key, oneshot::Sender<(Option<Value>, TransactionId)>),
    DeleteAsync(Key, oneshot::Sender<TransactionId>),
    PDelete(Key, oneshot::Sender<(KeyValuePairs, TransactionId)>),
    PDeleteAsync(Key, oneshot::Sender<TransactionId>),
    Ls(
        Option<Key>,
        oneshot::Sender<(Vec<RegularKeySegment>, TransactionId)>,
    ),
    LsAsync(Option<Key>, oneshot::Sender<TransactionId>),
    Subscribe(
        Key,
        UniqueFlag,
        oneshot::Sender<TransactionId>,
        mpsc::UnboundedSender<Option<Value>>,
        LiveOnlyFlag,
    ),
    SubscribeAsync(
        Key,
        UniqueFlag,
        oneshot::Sender<TransactionId>,
        LiveOnlyFlag,
    ),
    PSubscribe(
        Key,
        UniqueFlag,
        oneshot::Sender<TransactionId>,
        mpsc::UnboundedSender<PStateEvent>,
        Option<u64>,
        LiveOnlyFlag,
    ),
    PSubscribeAsync(
        Key,
        UniqueFlag,
        oneshot::Sender<TransactionId>,
        Option<u64>,
        LiveOnlyFlag,
    ),
    Unsubscribe(TransactionId),
    SubscribeLs(
        Option<Key>,
        oneshot::Sender<TransactionId>,
        mpsc::UnboundedSender<Vec<RegularKeySegment>>,
    ),
    SubscribeLsAsync(Option<Key>, oneshot::Sender<TransactionId>),
    UnsubscribeLs(TransactionId),
    AllMessages(mpsc::UnboundedSender<ServerMessage>),
}

enum ClientSocket {
    Tcp(TcpClientSocket),
    Ws(WsClientSocket),
}

impl ClientSocket {
    pub async fn send_msg(&mut self, msg: &ClientMessage) -> ConnectionResult<()> {
        match self {
            ClientSocket::Tcp(sock) => sock.send_msg(msg).await,
            ClientSocket::Ws(sock) => sock.send_msg(msg).await,
        }
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        match self {
            ClientSocket::Tcp(sock) => sock.receive_msg().await,
            ClientSocket::Ws(sock) => sock.receive_msg().await,
        }
    }
}

#[derive(Clone)]
pub struct Worterbuch {
    commands: mpsc::Sender<Command>,
    stop: mpsc::Sender<()>,
}

impl Worterbuch {
    fn new(commands: mpsc::Sender<Command>, stop: mpsc::Sender<()>) -> Self {
        Self { commands, stop }
    }

    pub async fn set_generic(&self, key: Key, value: Value) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Set(key, value, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let transaction_id = rx.await?;
        Ok(transaction_id)
    }

    pub async fn set<T: Serialize>(&self, key: Key, value: &T) -> ConnectionResult<TransactionId> {
        let value = json::to_value(value)?;
        self.set_generic(key, value).await
    }

    pub async fn publish_generic(&self, key: Key, value: Value) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Publish(key, value, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let transaction_id = rx.await?;
        Ok(transaction_id)
    }

    pub async fn publish<T: Serialize>(
        &self,
        key: Key,
        value: &T,
    ) -> ConnectionResult<TransactionId> {
        let value = json::to_value(value)?;
        self.publish_generic(key, value).await
    }

    pub async fn get_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::GetAsync(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    pub async fn get_generic(&self, key: Key) -> ConnectionResult<(Option<Value>, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Get(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        key: Key,
    ) -> ConnectionResult<(Option<T>, TransactionId)> {
        Ok(match self.get_generic(key).await? {
            (Some(val), tid) => (Some(json::from_value(val)?), tid),
            (None, tid) => (None, tid),
        })
    }

    pub async fn pget_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PGetAsync(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn pget_generic(&self, key: Key) -> ConnectionResult<(KeyValuePairs, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PGet(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let (kvps, tid) = rx.await?;
        Ok((kvps, tid))
    }

    pub async fn pget<T: DeserializeOwned>(
        &self,
        key: Key,
    ) -> ConnectionResult<(TypedKeyValuePairs<T>, TransactionId)> {
        let (kvps, tid) = self.pget_generic(key).await?;
        let typed_kvps = deserialize_key_value_pairs(kvps)?;
        Ok((typed_kvps, tid))
    }

    pub async fn delete_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::DeleteAsync(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn delete_generic(
        &self,
        key: Key,
    ) -> ConnectionResult<(Option<Value>, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Delete(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        match rx.await? {
            (Some(value), tid) => Ok((Some(value), tid)),
            (None, tid) => Ok((None, tid)),
        }
    }

    pub async fn delete<T: DeserializeOwned>(
        &self,
        key: Key,
    ) -> ConnectionResult<(Option<T>, TransactionId)> {
        Ok(match self.delete_generic(key).await? {
            (Some(val), tid) => (Some(json::from_value(val)?), tid),
            (None, tid) => (None, tid),
        })
    }

    pub async fn pdelete_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PDeleteAsync(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn pdelete_generic(
        &self,
        key: Key,
    ) -> ConnectionResult<(KeyValuePairs, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PDelete(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let (kvps, tid) = rx.await?;
        Ok((kvps, tid))
    }

    pub async fn pdelete<T: DeserializeOwned>(
        &self,
        key: Key,
    ) -> ConnectionResult<(TypedKeyValuePairs<T>, TransactionId)> {
        let (kvps, tid) = self.pdelete_generic(key).await?;
        let typed_kvps = deserialize_key_value_pairs(kvps)?;
        Ok((typed_kvps, tid))
    }

    pub async fn ls_async(&self, parent: Option<Key>) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::LsAsync(parent, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn ls(
        &self,
        parent: Option<Key>,
    ) -> ConnectionResult<(Vec<RegularKeySegment>, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Ls(parent, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let children = rx.await?;
        Ok(children)
    }

    pub async fn subscribe_async(
        &self,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::SubscribeAsync(key, unique, tx, live_only))
            .await?;
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn subscribe_generic(
        &self,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> ConnectionResult<(mpsc::UnboundedReceiver<Option<Value>>, TransactionId)> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (val_tx, val_rx) = mpsc::unbounded_channel();
        self.commands
            .send(Command::Subscribe(key, unique, tid_tx, val_tx, live_only))
            .await?;
        let transaction_id = tid_rx.await?;
        Ok((val_rx, transaction_id))
    }

    pub async fn subscribe<T: DeserializeOwned + Send + 'static>(
        &self,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> ConnectionResult<(mpsc::UnboundedReceiver<Option<T>>, TransactionId)> {
        let (val_rx, transaction_id) = self.subscribe_generic(key, unique, live_only).await?;
        let (typed_val_tx, typed_val_rx) = mpsc::unbounded_channel();
        spawn(deserialize_values(val_rx, typed_val_tx));
        Ok((typed_val_rx, transaction_id))
    }

    pub async fn psubscribe_async(
        &self,
        request_pattern: RequestPattern,
        unique: bool,
        live_only: bool,
        aggregation_duration: Option<Duration>,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::PSubscribeAsync(
                request_pattern,
                unique,
                tx,
                aggregation_duration.map(|d| d.as_millis() as u64),
                live_only,
            ))
            .await?;
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn psubscribe_generic(
        &self,
        request_pattern: RequestPattern,
        unique: bool,
        live_only: bool,
        aggregation_duration: Option<Duration>,
    ) -> ConnectionResult<(mpsc::UnboundedReceiver<PStateEvent>, TransactionId)> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        self.commands
            .send(Command::PSubscribe(
                request_pattern,
                unique,
                tid_tx,
                event_tx,
                aggregation_duration.map(|d| d.as_millis() as u64),
                live_only,
            ))
            .await?;
        let transaction_id = tid_rx.await?;
        Ok((event_rx, transaction_id))
    }

    pub async fn psubscribe<T: DeserializeOwned + Send + 'static>(
        &self,
        request_pattern: RequestPattern,
        unique: bool,
        live_only: bool,
        aggregation_duration: Option<Duration>,
    ) -> ConnectionResult<(mpsc::UnboundedReceiver<TypedStateEvents<T>>, TransactionId)> {
        let (event_rx, transaction_id) = self
            .psubscribe_generic(request_pattern, unique, live_only, aggregation_duration)
            .await?;
        let (typed_event_tx, typed_event_rx) = mpsc::unbounded_channel();
        spawn(deserialize_events(event_rx, typed_event_tx));
        Ok((typed_event_rx, transaction_id))
    }

    pub async fn unsubscribe(&self, transaction_id: TransactionId) -> ConnectionResult<()> {
        self.commands
            .send(Command::Unsubscribe(transaction_id))
            .await?;
        Ok(())
    }

    pub async fn subscribe_ls_async(&self, parent: Option<Key>) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::SubscribeLsAsync(parent, tx))
            .await?;
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn subscribe_ls(
        &self,
        parent: Option<Key>,
    ) -> ConnectionResult<(
        mpsc::UnboundedReceiver<Vec<RegularKeySegment>>,
        TransactionId,
    )> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (children_tx, children_rx) = mpsc::unbounded_channel();
        self.commands
            .send(Command::SubscribeLs(parent, tid_tx, children_tx))
            .await?;
        let transaction_id = tid_rx.await?;
        Ok((children_rx, transaction_id))
    }

    pub async fn unsubscribe_ls(&self, transaction_id: TransactionId) -> ConnectionResult<()> {
        self.commands
            .send(Command::UnsubscribeLs(transaction_id))
            .await?;
        Ok(())
    }

    pub async fn send_buffer(&self, delay: Duration) -> SendBuffer {
        SendBuffer::new(self.commands.clone(), delay).await
    }

    pub async fn close(&self) -> ConnectionResult<()> {
        self.stop.send(()).await?;
        Ok(())
    }

    pub async fn all_messages(&self) -> ConnectionResult<mpsc::UnboundedReceiver<ServerMessage>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.commands.send(Command::AllMessages(tx)).await?;
        Ok(rx)
    }
}

async fn deserialize_values<T: DeserializeOwned + Send + 'static>(
    mut val_rx: mpsc::UnboundedReceiver<Option<Value>>,
    typed_val_tx: mpsc::UnboundedSender<Option<T>>,
) {
    while let Some(val) = val_rx.recv().await {
        match val {
            Some(val) => match json::from_value(val) {
                Ok(typed_val) => {
                    if typed_val_tx.send(typed_val).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    log::error!("could not deserialize json to requested type: {e}");
                    break;
                }
            },
            None => {
                if typed_val_tx.send(None).is_err() {
                    break;
                }
            }
        };
    }
}

async fn deserialize_events<T: DeserializeOwned + Send + 'static>(
    mut event_rx: mpsc::UnboundedReceiver<PStateEvent>,
    typed_event_tx: mpsc::UnboundedSender<TypedStateEvents<T>>,
) {
    while let Some(evt) = event_rx.recv().await {
        match deserialize_pstate_event(evt) {
            Ok(typed_event) => {
                if typed_event_tx.send(typed_event).is_err() {
                    break;
                }
            }
            Result::Err(e) => {
                log::error!("could not deserialize json to requested type: {e}");
                break;
            }
        }
    }
}

#[derive(Default)]
struct Callbacks {
    all: Vec<mpsc::UnboundedSender<ServerMessage>>,
    get: HashMap<TransactionId, oneshot::Sender<(Option<Value>, TransactionId)>>,
    pget: HashMap<TransactionId, oneshot::Sender<(KeyValuePairs, TransactionId)>>,
    del: HashMap<TransactionId, oneshot::Sender<(Option<Value>, TransactionId)>>,
    pdel: HashMap<TransactionId, oneshot::Sender<(KeyValuePairs, TransactionId)>>,
    ls: HashMap<TransactionId, oneshot::Sender<(Vec<RegularKeySegment>, TransactionId)>>,
    sub: HashMap<TransactionId, mpsc::UnboundedSender<Option<Value>>>,
    psub: HashMap<TransactionId, mpsc::UnboundedSender<PStateEvent>>,
    subls: HashMap<TransactionId, mpsc::UnboundedSender<Vec<RegularKeySegment>>>,
}

struct TransactionIds {
    next_transaction_id: TransactionId,
}

impl Default for TransactionIds {
    fn default() -> Self {
        TransactionIds {
            next_transaction_id: 1,
        }
    }
}

impl TransactionIds {
    pub fn next(&mut self) -> TransactionId {
        let tid = self.next_transaction_id;
        self.next_transaction_id += 1;
        tid
    }
}

pub async fn connect_with_default_config<F: Future<Output = ()> + Send + 'static>(
    last_will: LastWill,
    grave_goods: GraveGoods,
    on_disconnect: F,
) -> ConnectionResult<(Worterbuch, Config)> {
    let config = Config::new();
    let conn = connect(config.clone(), last_will, grave_goods, on_disconnect).await?;
    Ok((conn, config))
}

pub async fn connect<F: Future<Output = ()> + Send + 'static>(
    config: Config,
    last_will: LastWill,
    grave_goods: GraveGoods,
    on_disconnect: F,
) -> ConnectionResult<Worterbuch> {
    let proto = &config.proto;
    let host_addr = &config.host_addr;
    let port = config.port;
    let tcp = proto == "tcp";
    let path = if tcp { "" } else { "/ws" };
    let url = format!("{proto}://{host_addr}:{port}{path}",);

    log::debug!("Got server url from config: {url}");

    if tcp {
        connect_tcp(
            host_addr.to_owned(),
            port,
            last_will,
            grave_goods,
            on_disconnect,
            config,
        )
        .await
    } else {
        connect_ws(url, last_will, grave_goods, on_disconnect, config).await
    }
}

async fn connect_ws<F: Future<Output = ()> + Send + 'static>(
    url: String,
    last_will: Vec<KeyValuePair>,
    grave_goods: Vec<String>,
    on_disconnect: F,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    log::debug!("Connecting to server {url} over websocket …");

    let (mut websocket, _) = connect_async(url).await?;
    log::debug!("Connected to server.");

    // TODO implement protocol versions properly
    let supported_protocol_versions = vec![ProtocolVersion { major: 0, minor: 6 }];

    let handshake = HandshakeRequest {
        supported_protocol_versions,
        last_will,
        grave_goods,
    };
    let msg = json::to_string(&CM::HandshakeRequest(handshake))?;
    log::debug!("Sending handshake message: {msg}");
    websocket.send(Message::Text(msg)).await?;

    match websocket.next().await {
        Some(Ok(msg)) => match msg.to_text() {
            Ok(data) => match json::from_str::<SM>(data) {
                Ok(SM::Handshake(handshake)) => {
                    log::debug!("Handhsake complete: {handshake}");
                    connected(
                        ClientSocket::Ws(WsClientSocket::new(websocket)),
                        on_disconnect,
                        config,
                    )
                }
                Ok(msg) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid handshake message: {msg:?}"),
                ))),
                Err(e) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid handshake message: {e}"),
                ))),
            },
            Err(e) => Err(ConnectionError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("server sent invalid handshake message: {e}"),
            ))),
        },
        Some(Err(e)) => Err(e.into()),
        None => Err(ConnectionError::IoError(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "connection closed before handshake",
        ))),
    }
}

async fn connect_tcp<F: Future<Output = ()> + Send + 'static>(
    host_addr: String,
    port: u16,
    last_will: Vec<KeyValuePair>,
    grave_goods: Vec<String>,
    on_disconnect: F,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    log::debug!("Connecting to server {host_addr}:{port} over tcp …");

    let stream = TcpStream::connect(format!("{host_addr}:{port}")).await?;
    let (tcp_rx, mut tcp_tx) = stream.into_split();
    let mut tcp_rx = BufReader::new(tcp_rx);

    log::debug!("Connected to server.");

    // TODO implement protocol versions properly
    let supported_protocol_versions = vec![ProtocolVersion { major: 0, minor: 6 }];

    let handshake = HandshakeRequest {
        supported_protocol_versions,
        last_will,
        grave_goods,
    };
    let mut msg = json::to_string(&CM::HandshakeRequest(handshake))?;
    msg.push('\n');
    log::debug!("Sending handshake message: {msg}");
    tcp_tx.write(msg.as_bytes()).await?;

    let mut line_buf = String::new();

    match tcp_rx.read_line(&mut line_buf).await {
        Ok(0) => {
            return Err(ConnectionError::IoError(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection closed before handshake",
            )))
        }
        Ok(_) => {
            let msg = json::from_str::<SM>(&line_buf);
            line_buf.clear();
            match msg {
                Ok(SM::Handshake(handshake)) => {
                    log::debug!("Handhsake complete: {handshake}");
                    connected(
                        ClientSocket::Tcp(TcpClientSocket::new(tcp_tx, tcp_rx.lines())),
                        on_disconnect,
                        config,
                    )
                }
                Ok(msg) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid handshake message: {msg:?}"),
                ))),
                Err(e) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid handshake message: {e}"),
                ))),
            }
        }
        Err(e) => Err(ConnectionError::IoError(e)),
    }
}

fn connected<F: Future<Output = ()> + Send + 'static>(
    client_socket: ClientSocket,
    on_disconnect: F,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    let (stop_tx, stop_rx) = mpsc::channel(1);
    let (cmd_tx, cmd_rx) = mpsc::channel(1);

    spawn(async move {
        run(cmd_rx, client_socket, stop_rx, config).await;
        log::info!("WebSocket closed.");
        on_disconnect.await;
    });

    Ok(Worterbuch::new(cmd_tx, stop_tx))
}

async fn run(
    mut cmd_rx: mpsc::Receiver<Command>,
    mut client_socket: ClientSocket,
    mut stop_rx: mpsc::Receiver<()>,
    config: Config,
) {
    let mut callbacks = Callbacks::default();
    let mut transaction_ids = TransactionIds::default();
    let mut last_keepalive_rx = Instant::now();
    let mut last_keepalive_tx = Instant::now();
    let mut keepalive_timer = interval(Duration::from_secs(1));
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        log::trace!("loop: wait for command / ws message / shutdown request");
        select! {
            _ = stop_rx.recv() => {
                log::info!("Shutdown request received.");
                break;
            },
            _ = keepalive_timer.tick() => {
                let lag = last_keepalive_tx - last_keepalive_rx;

                if lag >= Duration::from_secs(2) {
                    log::warn!("Server has been inactive for {} seconds", lag.as_secs());
                }
                if lag >= config.keepalive_timeout {
                    log::error!("Server has been inactive for too long. Disconnecting.");
                    break;
                }
                if last_keepalive_tx.elapsed().as_secs() >= 1 {
                    last_keepalive_tx = Instant::now();
                    if let Err(e) = send_keepalive(&mut client_socket, config.send_timeout).await {
                        log::error!("Error sending keepalive signal: {e}");
                        break;
                    }
                }
            },
            ws_msg = client_socket.receive_msg() => {
                last_keepalive_rx = Instant::now();
                match process_incoming_server_message(ws_msg, &mut callbacks).await {
                    Ok(ControlFlow::Break(_)) => break,
                    Err(e) => {
                        log::error!("Error processing server message: {e}");
                        break;
                    },
                    _ => log::trace!("websocket message processing done")
                }
            },
            cmd = cmd_rx.recv() => {
                match process_incoming_command(cmd, &mut callbacks, &mut transaction_ids).await {
                    Ok(ControlFlow::Continue(msg)) => if let Some(msg) = msg {
                        last_keepalive_tx = Instant::now();
                        if let Err(e) = send_with_timeout(&mut client_socket, &msg, config.send_timeout).await {
                            log::error!("Error sending message to server: {e}");
                            break;
                        }
                    },
                    Ok(ControlFlow::Break(_)) => break,
                    Err(e) => {
                        log::error!("Error processing command: {e}");
                        break;
                    },
                }
            }
        }
    }
}

async fn send_with_timeout(
    ws: &mut ClientSocket,
    msg: &ClientMessage,
    timeout: Duration,
) -> ConnectionResult<()> {
    select! {
        r = ws.send_msg (msg) => Ok(r?),
        _ = sleep(timeout) => Err(ConnectionError::Timeout),
    }
}

async fn process_incoming_command(
    cmd: Option<Command>,
    callbacks: &mut Callbacks,
    transaction_ids: &mut TransactionIds,
) -> ConnectionResult<ControlFlow<(), Option<CM>>> {
    if let Some(command) = cmd {
        log::debug!("Processing command: {command:?}");
        let transaction_id = transaction_ids.next();
        let cm = match command {
            Command::Set(key, value, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::Set(Set {
                    transaction_id,
                    key,
                    value,
                }))
            }
            Command::Publish(key, value, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::Publish(Publish {
                    transaction_id,
                    key,
                    value,
                }))
            }
            Command::Get(key, callback) => {
                callbacks.get.insert(transaction_id, callback);
                Some(CM::Get(Get {
                    transaction_id,
                    key,
                }))
            }
            Command::GetAsync(key, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::Get(Get {
                    transaction_id,
                    key,
                }))
            }
            Command::PGet(request_pattern, callback) => {
                callbacks.pget.insert(transaction_id, callback);
                Some(CM::PGet(PGet {
                    transaction_id,
                    request_pattern,
                }))
            }
            Command::PGetAsync(request_pattern, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::PGet(PGet {
                    transaction_id,
                    request_pattern,
                }))
            }
            Command::Delete(key, callback) => {
                callbacks.del.insert(transaction_id, callback);
                Some(CM::Delete(Delete {
                    transaction_id,
                    key,
                }))
            }
            Command::DeleteAsync(key, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::Delete(Delete {
                    transaction_id,
                    key,
                }))
            }
            Command::PDelete(request_pattern, callback) => {
                callbacks.pdel.insert(transaction_id, callback);
                Some(CM::PDelete(PDelete {
                    transaction_id,
                    request_pattern,
                }))
            }
            Command::PDeleteAsync(request_pattern, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::PDelete(PDelete {
                    transaction_id,
                    request_pattern,
                }))
            }
            Command::Ls(parent, callback) => {
                callbacks.ls.insert(transaction_id, callback);
                Some(CM::Ls(Ls {
                    transaction_id,
                    parent,
                }))
            }
            Command::LsAsync(parent, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::Ls(Ls {
                    transaction_id,
                    parent,
                }))
            }
            Command::Subscribe(key, unique, tid_callback, value_callback, live_only) => {
                callbacks.sub.insert(transaction_id, value_callback);
                tid_callback
                    .send(transaction_id)
                    .expect("error in callback");
                Some(CM::Subscribe(Subscribe {
                    transaction_id,
                    key,
                    unique,
                    live_only: Some(live_only),
                }))
            }
            Command::SubscribeAsync(key, unique, callback, live_only) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::Subscribe(Subscribe {
                    transaction_id,
                    key,
                    unique,
                    live_only: Some(live_only),
                }))
            }
            Command::PSubscribe(
                request_pattern,
                unique,
                tid_callback,
                event_callback,
                aggregate_events,
                live_only,
            ) => {
                callbacks.psub.insert(transaction_id, event_callback);
                tid_callback
                    .send(transaction_id)
                    .expect("error in callback");
                Some(CM::PSubscribe(PSubscribe {
                    transaction_id,
                    request_pattern,
                    unique,
                    aggregate_events,
                    live_only: Some(live_only),
                }))
            }
            Command::PSubscribeAsync(
                request_pattern,
                unique,
                callback,
                aggregate_events,
                live_only,
            ) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::PSubscribe(PSubscribe {
                    transaction_id,
                    request_pattern,
                    unique,
                    aggregate_events,
                    live_only: Some(live_only),
                }))
            }
            Command::Unsubscribe(transaction_id) => {
                callbacks.sub.remove(&transaction_id);
                callbacks.psub.remove(&transaction_id);
                Some(CM::Unsubscribe(Unsubscribe { transaction_id }))
            }
            Command::SubscribeLs(parent, tid_callback, children_callback) => {
                callbacks.subls.insert(transaction_id, children_callback);
                tid_callback
                    .send(transaction_id)
                    .expect("error in callback");
                Some(CM::SubscribeLs(SubscribeLs {
                    transaction_id,
                    parent,
                }))
            }
            Command::SubscribeLsAsync(parent, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::SubscribeLs(SubscribeLs {
                    transaction_id,
                    parent,
                }))
            }
            Command::UnsubscribeLs(transaction_id) => {
                callbacks.subls.remove(&transaction_id);
                Some(CM::UnsubscribeLs(UnsubscribeLs { transaction_id }))
            }
            Command::AllMessages(tx) => {
                callbacks.all.push(tx);
                None
            }
        };
        Ok(ControlFlow::Continue(cm))
    } else {
        log::debug!("No more commands");
        Ok(ControlFlow::Break(()))
    }
}

async fn process_incoming_server_message(
    msg: ConnectionResult<Option<ServerMessage>>,
    callbacks: &mut Callbacks,
) -> ConnectionResult<ControlFlow<()>> {
    match msg {
        Ok(Some(msg)) => {
            deliver_generic(&msg, callbacks);
            match msg {
                SM::Ack(_) => (),
                SM::State(state) => deliver_state(state, callbacks).await?,
                SM::PState(pstate) => deliver_pstate(pstate, callbacks).await?,
                SM::LsState(ls) => deliver_ls(ls, callbacks).await?,
                SM::Err(err) => deliver_err(err, callbacks).await,
                SM::Handshake(_) | SM::Keepalive => (),
            }
            Ok(ControlFlow::Continue(()))
        }
        Ok(None) => {
            log::warn!("Connection closed.");
            return Ok(ControlFlow::Break(()));
        }
        Err(e) => {
            log::error!("Error receiving message: {e}");
            return Ok(ControlFlow::Break(()));
        }
    }
}

fn deliver_generic(msg: &ServerMessage, callbacks: &mut Callbacks) {
    callbacks.all.retain(|tx| match tx.send(msg.clone()) {
        Ok(_) => true,
        Err(e) => {
            log::error!("Removing callback due to failure to deliver message to receiver: {e}");
            false
        }
    });
}

async fn deliver_state(state: State, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.get.remove(&state.transaction_id) {
        if let StateEvent::KeyValue(kvp) = &state.event {
            cb.send((Some(kvp.value.clone()), state.transaction_id))
                .expect("error in callback");
        }
    }
    if let Some(cb) = callbacks.del.remove(&state.transaction_id) {
        if let StateEvent::Deleted(kvp) = &state.event {
            cb.send((Some(kvp.value.clone()), state.transaction_id))
                .expect("error in callback");
        }
    }
    if let Some(cb) = callbacks.sub.get(&state.transaction_id) {
        let value = match state.event {
            StateEvent::KeyValue(kv) => Some(kv.value),
            StateEvent::Deleted(_) => None,
        };
        cb.send(value)?;
    }
    Ok(())
}

async fn deliver_pstate(pstate: PState, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.pget.remove(&pstate.transaction_id) {
        if let PStateEvent::KeyValuePairs(kvps) = &pstate.event {
            cb.send((kvps.clone(), pstate.transaction_id))
                .expect("error in callback");
        }
    }
    if let Some(cb) = callbacks.pdel.remove(&pstate.transaction_id) {
        if let PStateEvent::Deleted(kvps) = &pstate.event {
            cb.send((kvps.clone(), pstate.transaction_id))
                .expect("error in callback");
        }
    }
    if let Some(cb) = callbacks.psub.get(&pstate.transaction_id) {
        cb.send(pstate.event)?;
    }
    Ok(())
}

async fn deliver_ls(ls: LsState, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.ls.remove(&ls.transaction_id) {
        cb.send((ls.children.clone(), ls.transaction_id))
            .expect("error in callback");
    }
    if let Some(cb) = callbacks.subls.get(&ls.transaction_id) {
        cb.send(ls.children)?;
    }

    Ok(())
}

async fn deliver_err(err: Err, callbacks: &mut Callbacks) {
    if let Some(cb) = callbacks.get.remove(&err.transaction_id) {
        cb.send((None, err.transaction_id))
            .expect("error in callback");
    }
    if let Some(cb) = callbacks.del.remove(&err.transaction_id) {
        cb.send((None, err.transaction_id))
            .expect("error in callback");
    }
}

async fn send_keepalive(websocket: &mut ClientSocket, timeout: Duration) -> ConnectionResult<()> {
    log::trace!("Sending keepalive");
    send_with_timeout(websocket, &ClientMessage::Keepalive, timeout).await?;
    Ok(())
}

fn deserialize_key_value_pairs<T: DeserializeOwned>(
    kvps: KeyValuePairs,
) -> Result<TypedKeyValuePairs<T>, ConnectionError> {
    let mut typed = TypedKeyValuePairs::new();
    for kvp in kvps {
        typed.push(kvp.try_into()?);
    }
    Ok(typed)
}

fn deserialize_pstate_event<T: DeserializeOwned>(
    pstate: PStateEvent,
) -> Result<TypedStateEvents<T>, SubscriptionError> {
    Ok(pstate.try_into()?)
}
