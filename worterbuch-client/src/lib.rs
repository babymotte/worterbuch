/*
 *  Worterbuch client library
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

pub mod buffer;
pub mod config;
pub mod error;
pub mod tcp;
#[cfg(target_family = "unix")]
pub mod unix;
pub mod ws;

use crate::config::Config;
use buffer::SendBuffer;
use error::SubscriptionError;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{self as json};
use std::{
    collections::HashMap, future::Future, io, net::SocketAddr, ops::ControlFlow, time::Duration,
};
use tcp::TcpClientSocket;
#[cfg(target_family = "unix")]
use tokio::net::UnixStream;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    select, spawn,
    sync::{mpsc, oneshot},
    time::sleep,
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{handshake::client::generate_key, http::Request, Message},
};
#[cfg(target_family = "unix")]
use unix::UnixClientSocket;
use worterbuch_common::error::WorterbuchError;
use ws::WsClientSocket;

pub use worterbuch_common::*;
pub use worterbuch_common::{
    self,
    error::{ConnectionError, ConnectionResult},
    Ack, AuthorizationRequest, ClientMessage as CM, Delete, Err, Get, GraveGoods, Key,
    KeyValuePairs, LastWill, LsState, PState, PStateEvent, ProtocolVersion, RegularKeySegment,
    ServerMessage as SM, Set, State, StateEvent, TransactionId,
};

#[derive(Debug)]
pub(crate) enum Command {
    Set(Key, Value, oneshot::Sender<TransactionId>),
    SPubInit(Key, oneshot::Sender<TransactionId>),
    SPub(TransactionId, Value, oneshot::Sender<()>),
    Publish(Key, Value, oneshot::Sender<TransactionId>),
    Get(Key, oneshot::Sender<(Option<Value>, TransactionId)>),
    GetAsync(Key, oneshot::Sender<TransactionId>),
    PGet(Key, oneshot::Sender<(KeyValuePairs, TransactionId)>),
    PGetAsync(Key, oneshot::Sender<TransactionId>),
    Delete(Key, oneshot::Sender<(Option<Value>, TransactionId)>),
    DeleteAsync(Key, oneshot::Sender<TransactionId>),
    PDelete(Key, bool, oneshot::Sender<(KeyValuePairs, TransactionId)>),
    PDeleteAsync(Key, bool, oneshot::Sender<TransactionId>),
    Ls(
        Option<Key>,
        oneshot::Sender<(Vec<RegularKeySegment>, TransactionId)>,
    ),
    LsAsync(Option<Key>, oneshot::Sender<TransactionId>),
    PLs(
        Option<RequestPattern>,
        oneshot::Sender<(Vec<RegularKeySegment>, TransactionId)>,
    ),
    PLsAsync(Option<RequestPattern>, oneshot::Sender<TransactionId>),
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
    #[cfg(target_family = "unix")]
    Unix(UnixClientSocket),
}

impl ClientSocket {
    pub async fn send_msg(&mut self, msg: ClientMessage, wait: bool) -> ConnectionResult<()> {
        match self {
            ClientSocket::Tcp(sock) => sock.send_msg(msg, wait).await,
            ClientSocket::Ws(sock) => sock.send_msg(&msg).await,
            #[cfg(target_family = "unix")]
            ClientSocket::Unix(sock) => sock.send_msg(msg).await,
        }
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        match self {
            ClientSocket::Tcp(sock) => sock.receive_msg().await,
            ClientSocket::Ws(sock) => sock.receive_msg().await,
            #[cfg(target_family = "unix")]
            ClientSocket::Unix(sock) => sock.receive_msg().await,
        }
    }

    pub async fn close(self) -> ConnectionResult<()> {
        match self {
            ClientSocket::Tcp(tcp_client_socket) => tcp_client_socket.close().await?,
            ClientSocket::Ws(ws_client_socket) => ws_client_socket.close().await?,
            ClientSocket::Unix(unix_client_socket) => unix_client_socket.close().await?,
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Worterbuch {
    commands: mpsc::Sender<Command>,
    stop: mpsc::Sender<oneshot::Sender<()>>,
    client_id: String,
}

impl Worterbuch {
    fn new(
        commands: mpsc::Sender<Command>,
        stop: mpsc::Sender<oneshot::Sender<()>>,
        client_id: String,
    ) -> Self {
        Self {
            commands,
            stop,
            client_id,
        }
    }

    pub async fn set_last_will(
        &self,
        last_will: &[KeyValuePair],
    ) -> ConnectionResult<TransactionId> {
        self.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                &self.client_id,
                SYSTEM_TOPIC_LAST_WILL
            ),
            last_will,
        )
        .await
    }

    pub async fn set_grave_goods(&self, grave_goods: &[&str]) -> ConnectionResult<TransactionId> {
        self.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                &self.client_id,
                SYSTEM_TOPIC_GRAVE_GOODS
            ),
            grave_goods,
        )
        .await
    }

    pub async fn set_client_name<T: Serialize>(
        &self,
        client_name: T,
    ) -> ConnectionResult<TransactionId> {
        self.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                &self.client_id,
                SYSTEM_TOPIC_CLIENT_NAME
            ),
            client_name,
        )
        .await
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

    pub async fn set<T: Serialize>(&self, key: Key, value: T) -> ConnectionResult<TransactionId> {
        let value = json::to_value(value)?;
        self.set_generic(key, value).await
    }

    pub async fn spub_init(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::SPubInit(key, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let transaction_id = rx.await?;
        Ok(transaction_id)
    }

    pub async fn spub(&self, transaction_id: TransactionId, value: Value) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::SPub(transaction_id, value, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        rx.await?;
        Ok(())
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

    pub async fn pdelete_async(&self, key: Key, quiet: bool) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PDeleteAsync(key, quiet, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn pdelete_generic(
        &self,
        key: Key,
        quiet: bool,
    ) -> ConnectionResult<(KeyValuePairs, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PDelete(key, quiet, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let (kvps, tid) = rx.await?;
        Ok((kvps, tid))
    }

    pub async fn pdelete<T: DeserializeOwned>(
        &self,
        key: Key,
        quiet: bool,
    ) -> ConnectionResult<(TypedKeyValuePairs<T>, TransactionId)> {
        let (kvps, tid) = self.pdelete_generic(key, quiet).await?;
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

    pub async fn pls_async(
        &self,
        parent: Option<RequestPattern>,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PLsAsync(parent, tx);
        log::debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        log::debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    pub async fn pls(
        &self,
        parent: Option<RequestPattern>,
    ) -> ConnectionResult<(Vec<RegularKeySegment>, TransactionId)> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PLs(parent, tx);
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
    ) -> ConnectionResult<(mpsc::UnboundedReceiver<TypedPStateEvent<T>>, TransactionId)> {
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
        let (tx, rx) = oneshot::channel();
        self.stop.send(tx).await?;
        rx.await.ok();
        Ok(())
    }

    pub async fn all_messages(&self) -> ConnectionResult<mpsc::UnboundedReceiver<ServerMessage>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.commands.send(Command::AllMessages(tx)).await?;
        Ok(rx)
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
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
                    log::error!("could not deserialize json value to requested type: {e}");
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
    typed_event_tx: mpsc::UnboundedSender<TypedPStateEvent<T>>,
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

pub struct OnDisconnect {
    rx: oneshot::Receiver<()>,
}

impl Future for OnDisconnect {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.rx.poll_unpin(cx).map(|_| ())
    }
}

pub async fn connect_with_default_config() -> ConnectionResult<(Worterbuch, OnDisconnect, Config)> {
    let config = Config::new();
    let (conn, disconnected) = connect(config.clone()).await?;
    Ok((conn, disconnected, config))
}

pub async fn connect(config: Config) -> ConnectionResult<(Worterbuch, OnDisconnect)> {
    let mut err = None;
    for addr in &config.servers {
        log::info!("Trying to connect to server {addr} …");
        match try_connect(config.clone(), *addr).await {
            Ok(con) => {
                log::info!("Successfully connected to server {addr}");
                return Ok(con);
            }
            Err(e) => {
                log::warn!("Could not connect to server {addr}: {e}");
                err = Some(e);
            }
        }
    }
    if let Some(e) = err {
        Err(e)
    } else {
        Err(ConnectionError::NoServerAddressesConfigured)
    }
}

pub async fn try_connect(
    config: Config,
    host_addr: SocketAddr,
) -> ConnectionResult<(Worterbuch, OnDisconnect)> {
    let proto = &config.proto;
    let tcp = proto == "tcp";
    let unix = proto == "unix";
    let path = if tcp { "" } else { "/ws" };
    #[cfg(target_family = "unix")]
    let url = if unix {
        config
            .socket_path
            .clone()
            .unwrap_or_else(|| "/tmp/worterbuch.socket".into())
            .to_string_lossy()
            .to_string()
    } else {
        format!("{proto}://{host_addr}{path}")
    };
    #[cfg(not(target_family = "unix"))]
    let url = format!("{proto}://{host_addr}{path}");

    log::debug!("Got server url from config: {url}");

    let (disco_tx, disco_rx) = oneshot::channel();

    let wb = if tcp {
        connect_tcp(host_addr, disco_tx, config).await?
    } else if unix {
        #[cfg(target_family = "unix")]
        let wb = connect_unix(url, disco_tx, config).await?;
        #[cfg(not(target_family = "unix"))]
        let wb = panic!("not supported on non-unix operating systems");
        wb
    } else {
        connect_ws(url, disco_tx, config).await?
    };

    let disconnected = OnDisconnect { rx: disco_rx };

    Ok((wb, disconnected))
}

async fn connect_ws(
    url: String,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    log::debug!("Connecting to server {url} over websocket …");

    let auth_token = config.auth_token.clone();
    let mut request = Request::builder()
        .uri(url)
        .header("Sec-WebSocket-Protocol", "worterbuch".to_owned())
        .header("Sec-WebSocket-Key", generate_key());

    if let Some(auth_token) = auth_token {
        request = request.header("Authorization", format!("Bearer {auth_token}"));
    }
    let request: Request<()> = request.body(())?;

    let (mut websocket, _) = connect_async_with_config(request, None, true).await?;
    log::debug!("Connected to server.");

    let Welcome {
        client_id,
        info:
            ServerInfo {
                version: _,
                protocol_version,
                authorization_required,
            },
    } = match websocket.next().await {
        Some(Ok(msg)) => match msg.to_text() {
            Ok(data) => match json::from_str::<SM>(data) {
                Ok(SM::Welcome(welcome)) => {
                    log::debug!("Welcome message received: {welcome:?}");
                    welcome
                }
                Ok(msg) => {
                    return Err(ConnectionError::IoError(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("server sent invalid welcome message: {msg:?}"),
                    )))
                }
                Err(e) => {
                    return Err(ConnectionError::IoError(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("error parsing welcome message '{data}': {e}"),
                    )))
                }
            },
            Err(e) => {
                return Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid welcome message '{msg:?}': {e}"),
                )))
            }
        },
        Some(Err(e)) => return Err(e.into()),
        None => {
            return Err(ConnectionError::IoError(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "connection closed before welcome message",
            )))
        }
    };

    if authorization_required {
        if let Some(auth_token) = config.auth_token.clone() {
            let handshake = AuthorizationRequest { auth_token };
            let msg = json::to_string(&CM::AuthorizationRequest(handshake))?;
            log::debug!("Sending authorization message: {msg}");
            websocket.send(Message::Text(msg.into())).await?;

            match websocket.next().await {
                Some(Err(e)) => Err(e.into()),
                Some(Ok(Message::Text(msg))) => match serde_json::from_str(&msg) {
                    Ok(SM::Authorized(_)) => {
                        log::debug!("Authorization accepted.");
                        connected(
                            ClientSocket::Ws(WsClientSocket::new(websocket)),
                            on_disconnect,
                            config,
                            client_id,
                            protocol_version,
                        )
                    }
                    Ok(SM::Err(e)) => {
                        log::error!("Authorization failed: {e}");
                        Err(ConnectionError::WorterbuchError(
                            WorterbuchError::ServerResponse(e),
                        ))
                    }
                    Ok(msg) => Err(ConnectionError::IoError(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("server sent invalid authetication response: {msg:?}"),
                    ))),
                    Err(e) => Err(ConnectionError::IoError(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("error receiving authorization response: {e}"),
                    ))),
                },
                Some(Ok(msg)) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    format!("received unexpected message from server: {msg:?}"),
                ))),
                None => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before welcome message",
                ))),
            }
        } else {
            Err(ConnectionError::AuthorizationError(
                "Server requires authorization but no auth token was provided.".to_owned(),
            ))
        }
    } else {
        connected(
            ClientSocket::Ws(WsClientSocket::new(websocket)),
            on_disconnect,
            config,
            client_id,
            protocol_version,
        )
    }
}

async fn connect_tcp(
    host_addr: SocketAddr,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    let timeout = config.connection_timeout;
    log::debug!(
        "Connecting to server tcp://{host_addr} (timeout: {} ms) …",
        timeout.as_millis()
    );

    let stream = select! {
        conn = TcpStream::connect(host_addr) => conn,
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout("Timeout while waiting for TCP connection.".to_owned()));
        },
    }?;
    log::debug!("Connected to tcp://{host_addr}.");
    let (tcp_rx, mut tcp_tx) = stream.into_split();
    let mut tcp_rx = BufReader::new(tcp_rx).lines();

    log::debug!("Connected to server.");

    let Welcome {
        client_id,
        info:
            ServerInfo {
                version: _,
                protocol_version,
                authorization_required,
            },
    } = select! {
        line = tcp_rx.next_line() => match line {
            Ok(None) => {
                return Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before welcome message",
                )))
            }
            Ok(Some(line)) => {
                let msg = json::from_str::<SM>(&line);
                match msg {
                    Ok(SM::Welcome(welcome)) => {
                        log::debug!("Welcome message received: {welcome:?}");
                        welcome
                    }
                    Ok(msg) => {
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid welcome message: {msg:?}"),
                        )))
                    }
                    Err(e) => {
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error parsing welcome message '{line}': {e}"),
                        )))
                    }
                }
            }
            Err(e) => return Err(ConnectionError::IoError(e)),
        },
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout("Timeout while waiting for welcome message.".to_owned()));
        },
    };

    if authorization_required {
        if let Some(auth_token) = config.auth_token.clone() {
            let handshake = AuthorizationRequest { auth_token };
            let mut msg = json::to_string(&CM::AuthorizationRequest(handshake))?;
            msg.push('\n');
            log::debug!("Sending authorization message: {msg}");
            tcp_tx.write_all(msg.as_bytes()).await?;

            match tcp_rx.next_line().await {
                Ok(None) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before handshake",
                ))),
                Ok(Some(line)) => {
                    let msg = json::from_str::<SM>(&line);
                    match msg {
                        Ok(SM::Authorized(_)) => {
                            log::debug!("Authorization accepted.");
                            connected(
                                ClientSocket::Tcp(
                                    TcpClientSocket::new(
                                        tcp_tx,
                                        tcp_rx,
                                        config.send_timeout,
                                        config.channel_buffer_size,
                                    )
                                    .await,
                                ),
                                on_disconnect,
                                config,
                                client_id,
                                protocol_version,
                            )
                        }
                        Ok(SM::Err(e)) => {
                            log::error!("Authorization failed: {e}");
                            Err(ConnectionError::WorterbuchError(
                                WorterbuchError::ServerResponse(e),
                            ))
                        }
                        Ok(msg) => Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid authetication response: {msg:?}"),
                        ))),
                        Err(e) => Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error receiving authorization response: {e}"),
                        ))),
                    }
                }
                Err(e) => Err(ConnectionError::IoError(e)),
            }
        } else {
            Err(ConnectionError::AuthorizationError(
                "Server requires authorization but no auth token was provided.".to_owned(),
            ))
        }
    } else {
        connected(
            ClientSocket::Tcp(
                TcpClientSocket::new(
                    tcp_tx,
                    tcp_rx,
                    config.connection_timeout,
                    config.channel_buffer_size,
                )
                .await,
            ),
            on_disconnect,
            config,
            client_id,
            protocol_version,
        )
    }
}

#[cfg(target_family = "unix")]
async fn connect_unix(
    path: String,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    let timeout = config.connection_timeout;
    log::debug!(
        "Connecting to server socket {path} (timeout: {} ms) …",
        timeout.as_millis()
    );

    let stream = select! {
        conn = UnixStream::connect(&path) => conn,
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout("Timeout while waiting for TCP connection.".to_owned()));
        },
    }?;
    log::debug!("Connected to {path}.");
    let (tcp_rx, mut tcp_tx) = stream.into_split();
    let mut tcp_rx = BufReader::new(tcp_rx).lines();

    log::debug!("Connected to server.");

    let Welcome {
        client_id,
        info:
            ServerInfo {
                version: _,
                protocol_version,
                authorization_required,
            },
    } = select! {
        line = tcp_rx.next_line() => match line {
            Ok(None) => {
                return Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before welcome message",
                )))
            }
            Ok(Some(line)) => {
                let msg = json::from_str::<SM>(&line);
                match msg {
                    Ok(SM::Welcome(welcome)) => {
                        log::debug!("Welcome message received: {welcome:?}");
                        welcome
                    }
                    Ok(msg) => {
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid welcome message: {msg:?}"),
                        )))
                    }
                    Err(e) => {
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error parsing welcome message '{line}': {e}"),
                        )))
                    }
                }
            }
            Err(e) => return Err(ConnectionError::IoError(e)),
        },
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout("Timeout while waiting for welcome message.".to_owned()));
        },
    };

    if authorization_required {
        if let Some(auth_token) = config.auth_token.clone() {
            let handshake = AuthorizationRequest { auth_token };
            let mut msg = json::to_string(&CM::AuthorizationRequest(handshake))?;
            msg.push('\n');
            log::debug!("Sending authorization message: {msg}");
            tcp_tx.write_all(msg.as_bytes()).await?;

            match tcp_rx.next_line().await {
                Ok(None) => Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before handshake",
                ))),
                Ok(Some(line)) => {
                    let msg = json::from_str::<SM>(&line);
                    match msg {
                        Ok(SM::Authorized(_)) => {
                            log::debug!("Authorization accepted.");
                            connected(
                                ClientSocket::Unix(
                                    UnixClientSocket::new(
                                        tcp_tx,
                                        tcp_rx,
                                        config.send_timeout,
                                        config.channel_buffer_size,
                                    )
                                    .await,
                                ),
                                on_disconnect,
                                config,
                                client_id,
                                protocol_version,
                            )
                        }
                        Ok(SM::Err(e)) => {
                            log::error!("Authorization failed: {e}");
                            Err(ConnectionError::WorterbuchError(
                                WorterbuchError::ServerResponse(e),
                            ))
                        }
                        Ok(msg) => Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid authetication response: {msg:?}"),
                        ))),
                        Err(e) => Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error receiving authorization response: {e}"),
                        ))),
                    }
                }
                Err(e) => Err(ConnectionError::IoError(e)),
            }
        } else {
            Err(ConnectionError::AuthorizationError(
                "Server requires authorization but no auth token was provided.".to_owned(),
            ))
        }
    } else {
        connected(
            ClientSocket::Unix(
                UnixClientSocket::new(
                    tcp_tx,
                    tcp_rx,
                    config.send_timeout,
                    config.channel_buffer_size,
                )
                .await,
            ),
            on_disconnect,
            config,
            client_id,
            protocol_version,
        )
    }
}

fn connected(
    client_socket: ClientSocket,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
    client_id: String,
    protocol_version: ProtocolVersion,
) -> Result<Worterbuch, ConnectionError> {
    // TODO properly implement different protocol versions
    let supported_protocol_versions = [PROTOCOL_VERSION.to_owned()];

    if !supported_protocol_versions.contains(&protocol_version) {
        return Err(ConnectionError::WorterbuchError(
            WorterbuchError::ProtocolNegotiationFailed,
        ));
    }

    let (stop_tx, stop_rx) = mpsc::channel(1);
    let (cmd_tx, cmd_rx) = mpsc::channel(1);

    spawn(async move {
        if let Err(e) = run(cmd_rx, client_socket, stop_rx, config).await {
            log::error!("Connection closed with error: {e}");
        } else {
            log::debug!("Connection closed.");
        }
        on_disconnect.send(()).ok();
    });

    Ok(Worterbuch::new(cmd_tx, stop_tx, client_id))
}

async fn run(
    mut cmd_rx: mpsc::Receiver<Command>,
    mut client_socket: ClientSocket,
    mut stop_rx: mpsc::Receiver<oneshot::Sender<()>>,
    config: Config,
) -> ConnectionResult<()> {
    let mut callbacks = Callbacks::default();
    let mut transaction_ids = TransactionIds::default();

    let mut stop_tx = None;

    loop {
        log::trace!("loop: wait for command / ws message / shutdown request");
        select! {
            recv = stop_rx.recv() => {
                log::debug!("Shutdown request received.");
                stop_tx = recv;
                break;
            },
            ws_msg = client_socket.receive_msg() => {
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
                        if let Err(e) = client_socket.send_msg(msg, config.use_backpressure).await {
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

    client_socket.close().await?;
    if let Some(tx) = stop_tx {
        tx.send(()).ok();
    }

    Ok(())
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
            Command::SPubInit(key, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::SPubInit(SPubInit {
                    transaction_id,
                    key,
                }))
            }
            Command::SPub(transaction_id, value, callback) => {
                callback.send(()).expect("error in callback");
                Some(CM::SPub(SPub {
                    transaction_id,
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
            Command::PDelete(request_pattern, quiet, callback) => {
                callbacks.pdel.insert(transaction_id, callback);
                Some(CM::PDelete(PDelete {
                    transaction_id,
                    request_pattern,
                    quiet: Some(quiet),
                }))
            }
            Command::PDeleteAsync(request_pattern, quiet, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::PDelete(PDelete {
                    transaction_id,
                    request_pattern,
                    quiet: Some(quiet),
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
            Command::PLs(parent_pattern, callback) => {
                callbacks.ls.insert(transaction_id, callback);
                Some(CM::PLs(PLs {
                    transaction_id,
                    parent_pattern,
                }))
            }
            Command::PLsAsync(parent_pattern, callback) => {
                callback.send(transaction_id).expect("error in callback");
                Some(CM::PLs(PLs {
                    transaction_id,
                    parent_pattern,
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
                SM::State(state) => deliver_state(state, callbacks).await?,
                SM::PState(pstate) => deliver_pstate(pstate, callbacks).await?,
                SM::LsState(ls) => deliver_ls(ls, callbacks).await?,
                SM::Err(err) => deliver_err(err, callbacks).await,
                SM::Ack(_) | SM::Welcome(_) | SM::Authorized(_) => (),
            }
            Ok(ControlFlow::Continue(()))
        }
        Ok(None) => {
            log::warn!("Connection closed.");
            Ok(ControlFlow::Break(()))
        }
        Err(e) => {
            log::error!("Error receiving message: {e}");
            Ok(ControlFlow::Break(()))
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
        if let StateEvent::Value(v) = &state.event {
            cb.send((Some(v.clone()), state.transaction_id))
                .expect("error in callback");
        }
    }
    if let Some(cb) = callbacks.del.remove(&state.transaction_id) {
        if let StateEvent::Deleted(v) = &state.event {
            cb.send((Some(v.clone()), state.transaction_id))
                .expect("error in callback");
        }
    }
    if let Some(cb) = callbacks.sub.get(&state.transaction_id) {
        let value = match state.event {
            StateEvent::Value(v) => Some(v),
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
) -> Result<TypedPStateEvent<T>, SubscriptionError> {
    Ok(pstate.try_into()?)
}
