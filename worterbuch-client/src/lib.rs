pub mod buffer;
pub mod config;
pub mod error;

use crate::config::Config;
use buffer::SendBuffer;
use error::SubscriptionError;
use futures_util::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{self as json, Value};
use std::{collections::HashMap, future::Future, io, ops::ControlFlow, time::Duration};
use tokio::{
    net::TcpStream,
    select, spawn,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};

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
    PGet(Key, oneshot::Sender<(KeyValuePairs, TransactionId)>),
    Delete(Key, oneshot::Sender<(Option<Value>, TransactionId)>),
    PDelete(Key, oneshot::Sender<(KeyValuePairs, TransactionId)>),
    Ls(
        Option<Key>,
        oneshot::Sender<(Vec<RegularKeySegment>, TransactionId)>,
    ),
    Subscribe(
        Key,
        UniqueFlag,
        oneshot::Sender<TransactionId>,
        mpsc::Sender<Option<Value>>,
    ),
    PSubscribe(
        Key,
        UniqueFlag,
        oneshot::Sender<TransactionId>,
        mpsc::Sender<PStateEvent>,
    ),
    Unsubscribe(TransactionId),
    SubscribeLs(
        Option<Key>,
        oneshot::Sender<TransactionId>,
        mpsc::Sender<Vec<RegularKeySegment>>,
    ),
    UnsubscribeLs(TransactionId),
    Ack(mpsc::UnboundedSender<TransactionId>),
}

pub struct Worterbuch {
    commands: mpsc::Sender<Command>,
    stop: mpsc::Sender<()>,
}

impl Worterbuch {
    fn new(commands: mpsc::Sender<Command>, stop: mpsc::Sender<()>) -> Self {
        Self { commands, stop }
    }

    pub async fn set_generic(&mut self, key: Key, value: Value) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Set(key, value, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let transaction_id = rx.await?;
        Ok(transaction_id)
    }

    pub async fn set<T: Serialize>(
        &mut self,
        key: Key,
        value: &T,
    ) -> ConnectionResult<TransactionId> {
        let value = json::to_value(value)?;
        self.set_generic(key, value).await
    }

    pub async fn publish_generic(
        &mut self,
        key: Key,
        value: Value,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Publish(key, value, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let transaction_id = rx.await?;
        Ok(transaction_id)
    }

    pub async fn publish<T: Serialize>(
        &mut self,
        key: Key,
        value: &T,
    ) -> ConnectionResult<TransactionId> {
        let value = json::to_value(value)?;
        self.publish_generic(key, value).await
    }

    pub async fn get_generic(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(Option<Value>, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Get(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    pub async fn get<T: DeserializeOwned>(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(Option<T>, TransactionId)> {
        Ok(match self.get_generic(key).await? {
            (Some(val), tid) => (Some(json::from_value(val)?), tid),
            (None, tid) => (None, tid),
        })
    }

    pub async fn pget_generic(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(KeyValuePairs, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PGet(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let (kvps, tid) = rx.await?;
        Ok((kvps, tid))
    }

    pub async fn pget<T: DeserializeOwned>(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(TypedKeyValuePairs<T>, TransactionId)> {
        let (kvps, tid) = self.pget_generic(key).await?;
        let typed_kvps = deserialize_key_value_pairs(kvps)?;
        Ok((typed_kvps, tid))
    }

    pub async fn delete_generic(
        &mut self,
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
        &mut self,
        key: Key,
    ) -> ConnectionResult<(Option<T>, TransactionId)> {
        Ok(match self.delete_generic(key).await? {
            (Some(val), tid) => (Some(json::from_value(val)?), tid),
            (None, tid) => (None, tid),
        })
    }

    pub async fn pdelete_generic(
        &mut self,
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
        &mut self,
        key: Key,
    ) -> ConnectionResult<(TypedKeyValuePairs<T>, TransactionId)> {
        let (kvps, tid) = self.pdelete_generic(key).await?;
        let typed_kvps = deserialize_key_value_pairs(kvps)?;
        Ok((typed_kvps, tid))
    }

    pub async fn ls(
        &mut self,
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

    pub async fn subscribe_generic(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(mpsc::Receiver<Option<Value>>, TransactionId)> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (val_tx, val_rx) = mpsc::channel(1);
        self.commands
            .send(Command::Subscribe(key, false, tid_tx, val_tx))
            .await?;
        let transaction_id = tid_rx.await?;
        Ok((val_rx, transaction_id))
    }

    pub async fn subscribe<T: DeserializeOwned + Send + 'static>(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(mpsc::Receiver<Option<T>>, TransactionId)> {
        let (val_rx, transaction_id) = self.subscribe_generic(key).await?;
        let (typed_val_tx, typed_val_rx) = mpsc::channel(1);
        spawn(deserialize_values(val_rx, typed_val_tx));
        Ok((typed_val_rx, transaction_id))
    }

    pub async fn subscribe_unique_generic(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(mpsc::Receiver<Option<Value>>, TransactionId)> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (val_tx, val_rx) = mpsc::channel(1);
        self.commands
            .send(Command::Subscribe(key, true, tid_tx, val_tx))
            .await?;
        let transaction_id = tid_rx.await?;
        Ok((val_rx, transaction_id))
    }

    pub async fn subscribe_unique<T: DeserializeOwned + Send + 'static>(
        &mut self,
        key: Key,
    ) -> ConnectionResult<(mpsc::Receiver<Option<T>>, TransactionId)> {
        let (val_rx, transaction_id) = self.subscribe_unique_generic(key).await?;
        let (typed_val_tx, typed_val_rx) = mpsc::channel(1);
        spawn(deserialize_values(val_rx, typed_val_tx));
        Ok((typed_val_rx, transaction_id))
    }

    pub async fn psubscribe_generic(
        &mut self,
        request_pattern: RequestPattern,
    ) -> ConnectionResult<(mpsc::Receiver<PStateEvent>, TransactionId)> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (event_tx, event_rx) = mpsc::channel(1);
        self.commands
            .send(Command::PSubscribe(
                request_pattern,
                false,
                tid_tx,
                event_tx,
            ))
            .await?;
        let transaction_id = tid_rx.await?;
        Ok((event_rx, transaction_id))
    }

    pub async fn psubscribe<T: DeserializeOwned + Send + 'static>(
        &mut self,
        request_pattern: RequestPattern,
    ) -> ConnectionResult<(mpsc::Receiver<TypedStateEvents<T>>, TransactionId)> {
        let (event_rx, transaction_id) = self.psubscribe_generic(request_pattern).await?;
        let (typed_event_tx, typed_event_rx) = mpsc::channel(1);
        spawn(deserialize_events(event_rx, typed_event_tx));
        Ok((typed_event_rx, transaction_id))
    }

    pub async fn psubscribe_unique_generic(
        &mut self,
        request_pattern: RequestPattern,
    ) -> ConnectionResult<(mpsc::Receiver<PStateEvent>, TransactionId)> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (event_tx, event_rx) = mpsc::channel(1);
        self.commands
            .send(Command::PSubscribe(request_pattern, true, tid_tx, event_tx))
            .await?;
        let transaction_id = tid_rx.await?;
        Ok((event_rx, transaction_id))
    }

    pub async fn psubscribe_unique<T: DeserializeOwned + Send + 'static>(
        &mut self,
        request_pattern: RequestPattern,
    ) -> ConnectionResult<(mpsc::Receiver<TypedStateEvents<T>>, TransactionId)> {
        let (event_rx, transaction_id) = self.psubscribe_unique_generic(request_pattern).await?;
        let (typed_event_tx, typed_event_rx) = mpsc::channel(1);
        spawn(deserialize_events(event_rx, typed_event_tx));
        Ok((typed_event_rx, transaction_id))
    }

    pub async fn unsubscribe(&self, transaction_id: TransactionId) -> ConnectionResult<()> {
        self.commands
            .send(Command::Unsubscribe(transaction_id))
            .await?;
        Ok(())
    }

    pub async fn subscribe_ls(
        &mut self,
        parent: Option<Key>,
    ) -> ConnectionResult<(mpsc::Receiver<Vec<RegularKeySegment>>, TransactionId)> {
        let (tid_tx, tid_rx) = oneshot::channel();
        let (children_tx, children_rx) = mpsc::channel(1);
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

    pub async fn close(&mut self) -> ConnectionResult<()> {
        self.stop.send(()).await?;
        Ok(())
    }

    pub async fn acks(&mut self) -> ConnectionResult<mpsc::UnboundedReceiver<TransactionId>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.commands.send(Command::Ack(tx)).await?;
        Ok(rx)
    }
}

async fn deserialize_values<T: DeserializeOwned + Send + 'static>(
    mut val_rx: mpsc::Receiver<Option<Value>>,
    typed_val_tx: mpsc::Sender<Option<T>>,
) {
    while let Some(val) = val_rx.recv().await {
        match val {
            Some(val) => match json::from_value(val) {
                Ok(typed_val) => {
                    if typed_val_tx.send(typed_val).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    log::error!("could not deserialize json to requested type: {e}");
                    break;
                }
            },
            None => {
                if typed_val_tx.send(None).await.is_err() {
                    break;
                }
            }
        };
    }
}

async fn deserialize_events<T: DeserializeOwned + Send + 'static>(
    mut event_rx: mpsc::Receiver<PStateEvent>,
    typed_event_tx: mpsc::Sender<TypedStateEvents<T>>,
) {
    while let Some(evt) = event_rx.recv().await {
        match deserialize_pstate_event(evt) {
            Ok(typed_event) => {
                if typed_event_tx.send(typed_event).await.is_err() {
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
    ack: Vec<mpsc::UnboundedSender<TransactionId>>,
    get: HashMap<TransactionId, oneshot::Sender<(Option<Value>, TransactionId)>>,
    pget: HashMap<TransactionId, oneshot::Sender<(KeyValuePairs, TransactionId)>>,
    del: HashMap<TransactionId, oneshot::Sender<(Option<Value>, TransactionId)>>,
    pdel: HashMap<TransactionId, oneshot::Sender<(KeyValuePairs, TransactionId)>>,
    ls: HashMap<TransactionId, oneshot::Sender<(Vec<RegularKeySegment>, TransactionId)>>,
    sub: HashMap<TransactionId, mpsc::Sender<Option<Value>>>,
    psub: HashMap<TransactionId, mpsc::Sender<PStateEvent>>,
    subls: HashMap<TransactionId, mpsc::Sender<Vec<RegularKeySegment>>>,
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
    let config = Config::new()?;
    let conn = connect(
        &config.proto,
        &config.host_addr,
        config.port,
        last_will,
        grave_goods,
        on_disconnect,
    )
    .await?;
    Ok((conn, config))
}

pub async fn connect<F: Future<Output = ()> + Send + 'static>(
    proto: &str,
    host_addr: &str,
    port: u16,
    last_will: LastWill,
    grave_goods: GraveGoods,
    on_disconnect: F,
) -> ConnectionResult<Worterbuch> {
    let url = format!("{proto}://{host_addr}:{port}/ws");
    log::debug!("Connecting to server {url} …");
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
                    connected(websocket, on_disconnect)
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

type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn connected<F: Future<Output = ()> + Send + 'static>(
    websocket: WebSocket,
    on_disconnect: F,
) -> Result<Worterbuch, ConnectionError> {
    let (stop_tx, stop_rx) = mpsc::channel(1);
    let (cmd_tx, cmd_rx) = mpsc::channel(1);

    spawn(async move {
        run(cmd_rx, websocket, stop_rx).await;
        log::info!("WebSocket closed.");
        on_disconnect.await;
    });

    Ok(Worterbuch::new(cmd_tx, stop_tx))
}

async fn run(
    mut cmd_rx: mpsc::Receiver<Command>,
    mut websocket: WebSocket,
    mut stop_rx: mpsc::Receiver<()>,
) {
    let mut callbacks = Callbacks::default();
    let mut transaction_ids = TransactionIds::default();
    loop {
        log::debug!("loop: wait for command / ws message / shutdown request");
        select! {
            cmd = cmd_rx.recv() => match process_incoming_command(cmd, &mut callbacks, &mut transaction_ids, &mut websocket).await {
                Ok(ControlFlow::Break(_)) => break,
                Err(e) => {
                    log::error!("Error processing command: {e}");
                    break;
                },
                _ => log::debug!("command processing done")
            },
            ws_msg = websocket.next() => match process_incoming_server_message(ws_msg, &mut callbacks).await {
                Ok(ControlFlow::Break(_)) => break,
                Err(e) => {
                    log::error!("Error processing server message: {e}");
                    break;
                },
                _ => log::debug!("websocket message processing done")
            },
            _ = stop_rx.recv() => {
                log::info!("Shutdown request received.");
                break;
            },
        }
    }
}

async fn process_incoming_command(
    cmd: Option<Command>,
    callbacks: &mut Callbacks,
    transaction_ids: &mut TransactionIds,
    websocket: &mut WebSocket,
) -> ConnectionResult<ControlFlow<()>> {
    if let Some(command) = cmd {
        log::debug!("Processing command: {command:?}");
        let tid = transaction_ids.next();
        match command {
            Command::Set(key, value, callback) => {
                set(tid, key, value, websocket, callback).await?;
            }
            Command::Publish(key, value, callback) => {
                publish(tid, key, value, websocket, callback).await?;
            }
            Command::Get(key, callback) => {
                get(tid, key, websocket, callback, callbacks).await?;
            }
            Command::PGet(request_pattern, callback) => {
                pget(tid, request_pattern, websocket, callback, callbacks).await?;
            }
            Command::Delete(key, callback) => {
                delete(tid, key, websocket, callback, callbacks).await?;
            }
            Command::PDelete(request_pattern, callback) => {
                pdelete(tid, request_pattern, websocket, callback, callbacks).await?;
            }
            Command::Ls(parent, callback) => {
                ls(tid, parent, websocket, callback, callbacks).await?;
            }
            Command::Subscribe(key, unique, tid_callback, value_callback) => {
                subscribe(
                    tid,
                    key,
                    unique,
                    tid_callback,
                    value_callback,
                    websocket,
                    callbacks,
                )
                .await?;
            }
            Command::PSubscribe(request_pattern, unique, tid_callback, event_callback) => {
                psubscribe(
                    tid,
                    request_pattern,
                    unique,
                    tid_callback,
                    event_callback,
                    websocket,
                    callbacks,
                )
                .await?;
            }
            Command::Unsubscribe(transaction_id) => {
                unsubscribe(transaction_id, websocket, callbacks).await?;
            }
            Command::SubscribeLs(parent, tid_callback, children_callback) => {
                subscribels(
                    tid,
                    parent,
                    tid_callback,
                    children_callback,
                    websocket,
                    callbacks,
                )
                .await?;
            }
            Command::UnsubscribeLs(transaction_id) => {
                unsubscribels(transaction_id, websocket, callbacks).await?;
            }
            Command::Ack(tx) => {
                callbacks.ack.push(tx);
            }
        }
    } else {
        log::debug!("No more commands");
        return Ok(ControlFlow::Break(()));
    }

    Ok(ControlFlow::Continue(()))
}

async fn set(
    transaction_id: TransactionId,
    key: Key,
    value: Value,
    websocket: &mut WebSocket,
    callback: oneshot::Sender<u64>,
) -> Result<(), ConnectionError> {
    let msg = CM::Set(Set {
        transaction_id,
        key,
        value,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callback.send(transaction_id).expect("error in callback");
    Ok(())
}

async fn publish(
    transaction_id: TransactionId,
    key: Key,
    value: Value,
    websocket: &mut WebSocket,
    callback: oneshot::Sender<u64>,
) -> Result<(), ConnectionError> {
    let msg = CM::Publish(Publish {
        transaction_id,
        key,
        value,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callback.send(transaction_id).expect("error in callback");
    Ok(())
}

async fn get(
    transaction_id: TransactionId,
    key: Key,
    websocket: &mut WebSocket,
    callback: oneshot::Sender<(Option<Value>, TransactionId)>,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::Get(Get {
        transaction_id,
        key,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.get.insert(transaction_id, callback);
    Ok(())
}

async fn pget(
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    websocket: &mut WebSocket,
    callback: oneshot::Sender<(KeyValuePairs, TransactionId)>,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::PGet(PGet {
        transaction_id,
        request_pattern,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.pget.insert(transaction_id, callback);
    Ok(())
}

async fn delete(
    transaction_id: TransactionId,
    key: String,
    websocket: &mut WebSocket,
    callback: oneshot::Sender<(Option<Value>, TransactionId)>,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::Delete(Delete {
        transaction_id,
        key,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.del.insert(transaction_id, callback);
    Ok(())
}

async fn pdelete(
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    websocket: &mut WebSocket,
    callback: oneshot::Sender<(KeyValuePairs, TransactionId)>,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::PDelete(PDelete {
        transaction_id,
        request_pattern,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.pdel.insert(transaction_id, callback);
    Ok(())
}

async fn ls(
    transaction_id: TransactionId,
    parent: Option<Key>,
    websocket: &mut WebSocket,
    callback: oneshot::Sender<(Vec<RegularKeySegment>, TransactionId)>,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::Ls(Ls {
        transaction_id,
        parent,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.ls.insert(transaction_id, callback);
    Ok(())
}

async fn subscribe(
    transaction_id: TransactionId,
    key: Key,
    unique: UniqueFlag,
    tid_callback: oneshot::Sender<TransactionId>,
    value_callback: mpsc::Sender<Option<Value>>,
    websocket: &mut WebSocket,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::Subscribe(Subscribe {
        transaction_id,
        key,
        unique,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.sub.insert(transaction_id, value_callback);
    tid_callback
        .send(transaction_id)
        .expect("error in callback");
    Ok(())
}

async fn psubscribe(
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    unique: UniqueFlag,
    tid_callback: oneshot::Sender<TransactionId>,
    event_callback: mpsc::Sender<PStateEvent>,
    websocket: &mut WebSocket,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::PSubscribe(PSubscribe {
        transaction_id,
        request_pattern,
        unique,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.psub.insert(transaction_id, event_callback);
    tid_callback
        .send(transaction_id)
        .expect("error in callback");
    Ok(())
}

async fn unsubscribe(
    transaction_id: TransactionId,
    websocket: &mut WebSocket,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::Unsubscribe(Unsubscribe { transaction_id });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.sub.remove(&transaction_id);
    callbacks.psub.remove(&transaction_id);
    Ok(())
}

async fn subscribels(
    transaction_id: TransactionId,
    parent: Option<Key>,
    tid_callback: oneshot::Sender<TransactionId>,
    children_callback: mpsc::Sender<Vec<RegularKeySegment>>,
    websocket: &mut WebSocket,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::SubscribeLs(SubscribeLs {
        transaction_id,
        parent,
    });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.subls.insert(transaction_id, children_callback);
    tid_callback
        .send(transaction_id)
        .expect("error in callback");
    Ok(())
}

async fn unsubscribels(
    transaction_id: TransactionId,
    websocket: &mut WebSocket,
    callbacks: &mut Callbacks,
) -> Result<(), ConnectionError> {
    let msg = CM::UnsubscribeLs(UnsubscribeLs { transaction_id });
    let json = json::to_string(&msg)?;
    log::debug!("Sending message: {json}");
    let ws_msg = Message::Text(json);
    websocket.send(ws_msg).await?;
    callbacks.subls.remove(&transaction_id);
    Ok(())
}

async fn process_incoming_server_message(
    msg: Option<Result<Message, tungstenite::Error>>,
    callbacks: &mut Callbacks,
) -> ConnectionResult<ControlFlow<()>> {
    if let Some(Ok(msg)) = msg {
        if let Message::Text(json) = msg {
            log::debug!("Received message: {json}");
            match json::from_str::<SM>(&json) {
                Ok(msg) => match msg {
                    SM::Ack(ack) => deliver_ack(ack, callbacks).await?,
                    SM::State(state) => deliver_state(state, callbacks).await?,
                    SM::PState(pstate) => deliver_pstate(pstate, callbacks).await?,
                    SM::LsState(ls) => deliver_ls(ls, callbacks).await?,
                    SM::Err(err) => deliver_err(err, callbacks).await,
                    SM::Handshake(_) => (),
                },
                Err(e) => {
                    log::error!("Error decoding message: {e}");
                }
            }
        }
    } else {
        log::error!("Connection to server lost.");
        return Ok(ControlFlow::Break(()));
    }

    Ok(ControlFlow::Continue(()))
}

async fn deliver_ack(ack: Ack, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    for tx in &callbacks.ack {
        tx.send(ack.transaction_id)?;
    }
    Ok(())
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
        cb.send(value).await?;
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
        cb.send(pstate.event).await?;
    }
    Ok(())
}

async fn deliver_ls(ls: LsState, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.ls.remove(&ls.transaction_id) {
        cb.send((ls.children.clone(), ls.transaction_id))
            .expect("error in callback");
    }
    if let Some(cb) = callbacks.subls.get(&ls.transaction_id) {
        cb.send(ls.children).await?;
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
