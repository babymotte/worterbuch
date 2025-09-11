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
#[cfg(feature = "tcp")]
pub mod tcp;
#[cfg(all(target_family = "unix", feature = "unix"))]
pub mod unix;
#[cfg(any(feature = "ws", feature = "wasm"))]
pub mod ws;

use crate::config::Config;
use buffer::SendBuffer;
use error::SubscriptionError;
use futures_util::FutureExt;
#[cfg(any(feature = "ws", feature = "wasm"))]
use futures_util::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{self as json};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::Future,
    io,
    net::SocketAddr,
    ops::ControlFlow,
    time::Duration,
};
#[cfg(feature = "tcp")]
use tcp::TcpClientSocket;
#[cfg(feature = "tcp")]
use tokio::net::TcpStream;
#[cfg(all(target_family = "unix", feature = "unix"))]
use tokio::net::UnixStream;
#[cfg(any(feature = "tcp", feature = "unix"))]
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    time::sleep,
};
#[cfg(feature = "tokio")]
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot},
};
#[cfg(feature = "ws")]
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, handshake::client::generate_key, http::Request},
};
#[cfg(feature = "wasm")]
use tokio_tungstenite_wasm::{Message, connect as connect_wasm};
use tracing::{Level, debug, error, info, instrument, trace, warn};
#[cfg(all(target_family = "unix", feature = "unix"))]
use unix::UnixClientSocket;
use worterbuch_common::error::WorterbuchError;
#[cfg(any(feature = "ws", feature = "wasm"))]
use ws::WsClientSocket;

pub use worterbuch_common::*;
pub use worterbuch_common::{
    self, Ack, AuthorizationRequest, ClientMessage as CM, Delete, Err, Get, GraveGoods, Key,
    KeyValuePairs, LastWill, LsState, PState, PStateEvent, ProtocolVersion, RegularKeySegment,
    ServerMessage as SM, Set, State, StateEvent, TransactionId,
    error::{ConnectionError, ConnectionResult},
};

const PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::new(1, 1);

#[derive(Debug)]
pub(crate) enum Command {
    Set(Key, Value, AckCallback),
    SetAsync(Key, Value, AsyncTicket),
    CSet(Key, Value, CasVersion, AckCallback),
    CSetAsync(Key, Value, CasVersion, AsyncTicket),
    SPubInit(Key, AckCallback),
    SPubInitAsync(Key, AsyncTicket),
    SPub(TransactionId, Value, AckCallback),
    SPubAsync(TransactionId, Value, AsyncTicket),
    Publish(Key, Value, AckCallback),
    PublishAsync(Key, Value, AsyncTicket),
    Get(Key, StateCallback),
    GetAsync(Key, AsyncTicket),
    CGet(Key, CStateCallback),
    CGetAsync(Key, AsyncTicket),
    PGet(Key, PStateCallback),
    PGetAsync(Key, AsyncTicket),
    Delete(Key, StateCallback),
    DeleteAsync(Key, AsyncTicket),
    PDelete(Key, bool, PStateCallback),
    PDeleteAsync(Key, bool, AsyncTicket),
    Ls(Option<Key>, LsStateCallback),
    LsAsync(Option<Key>, AsyncTicket),
    PLs(Option<RequestPattern>, LsStateCallback),
    PLsAsync(Option<RequestPattern>, AsyncTicket),
    Subscribe(
        Key,
        UniqueFlag,
        AckCallback,
        mpsc::UnboundedSender<Option<Value>>,
        LiveOnlyFlag,
    ),
    SubscribeAsync(Key, UniqueFlag, AsyncTicket, LiveOnlyFlag),
    PSubscribe(
        Key,
        UniqueFlag,
        AckCallback,
        mpsc::UnboundedSender<PStateEvent>,
        Option<u64>,
        LiveOnlyFlag,
    ),
    PSubscribeAsync(Key, UniqueFlag, AsyncTicket, Option<u64>, LiveOnlyFlag),
    Unsubscribe(TransactionId, AckCallback),
    UnsubscribeAsync(TransactionId, AsyncTicket),
    SubscribeLs(
        Option<Key>,
        AckCallback,
        mpsc::UnboundedSender<Vec<RegularKeySegment>>,
    ),
    SubscribeLsAsync(Option<Key>, AsyncTicket),
    UnsubscribeLs(TransactionId, AckCallback),
    UnsubscribeLsAsync(TransactionId, AsyncTicket),
    Lock(Key, AckCallback),
    LockAsync(Key, AsyncTicket),
    AcquireLock(Key, AckCallback),
    ReleaseLock(Key, AckCallback),
    ReleaseLockAsync(Key, AsyncTicket),
    AllMessages(GenericCallback),
}

enum ClientSocket {
    #[cfg(feature = "tcp")]
    Tcp(TcpClientSocket),
    #[cfg(any(feature = "ws", feature = "wasm"))]
    Ws(WsClientSocket),
    #[cfg(all(target_family = "unix", feature = "unix"))]
    Unix(UnixClientSocket),
}

impl ClientSocket {
    #[instrument(skip(self), level = "trace", err)]
    pub async fn send_msg(&mut self, msg: CM, wait: bool) -> ConnectionResult<()> {
        match self {
            #[cfg(feature = "tcp")]
            ClientSocket::Tcp(sock) => sock.send_msg(msg, wait).await,
            #[cfg(any(feature = "ws", feature = "wasm"))]
            ClientSocket::Ws(sock) => sock.send_msg(&msg).await,
            #[cfg(all(target_family = "unix", feature = "unix"))]
            ClientSocket::Unix(sock) => sock.send_msg(msg).await,
        }
    }

    #[instrument(skip(self), level = "trace", err)]
    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        match self {
            #[cfg(feature = "tcp")]
            ClientSocket::Tcp(sock) => sock.receive_msg().await,
            #[cfg(any(feature = "ws", feature = "wasm"))]
            ClientSocket::Ws(sock) => sock.receive_msg().await,
            #[cfg(all(target_family = "unix", feature = "unix"))]
            ClientSocket::Unix(sock) => sock.receive_msg().await,
        }
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn close(self) -> ConnectionResult<()> {
        match self {
            #[cfg(feature = "tcp")]
            ClientSocket::Tcp(tcp_client_socket) => tcp_client_socket.close().await?,
            #[cfg(any(feature = "ws", feature = "wasm"))]
            ClientSocket::Ws(ws_client_socket) => ws_client_socket.close().await?,
            #[cfg(all(target_family = "unix", feature = "unix"))]
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

    #[instrument(skip(self), err)]
    pub async fn set_last_will(&self, last_will: &[KeyValuePair]) -> ConnectionResult<()> {
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

    #[instrument(skip(self), err)]
    pub async fn set_grave_goods(&self, grave_goods: &[&str]) -> ConnectionResult<()> {
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

    #[instrument(skip(self), err)]
    pub async fn set_client_name<T: Display + Debug>(
        &self,
        client_name: T,
    ) -> ConnectionResult<()> {
        self.set(
            topic!(
                SYSTEM_TOPIC_ROOT,
                SYSTEM_TOPIC_CLIENTS,
                &self.client_id,
                SYSTEM_TOPIC_CLIENT_NAME
            ),
            client_name.to_string(),
        )
        .await
    }

    #[instrument(skip(self), err)]
    pub async fn set_generic(&self, key: Key, value: Value) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Set(key, value, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn set<T: Serialize>(&self, key: Key, value: T) -> ConnectionResult<()> {
        let value = json::to_value(value)?;
        self.set_generic(key, value).await
    }

    #[instrument(skip(self), err)]
    pub async fn set_generic_async(
        &self,
        key: Key,
        value: Value,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::SetAsync(key, value, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn set_async<T: Serialize>(
        &self,
        key: Key,
        value: T,
    ) -> ConnectionResult<TransactionId> {
        let value = json::to_value(value)?;
        self.set_generic_async(key, value).await
    }

    #[instrument(skip(self), err)]
    pub async fn cset_generic(
        &self,
        key: Key,
        value: Value,
        version: CasVersion,
    ) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::CSet(key, value, version, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn cset<T: Serialize>(
        &self,
        key: Key,
        value: T,
        version: CasVersion,
    ) -> ConnectionResult<()> {
        let value = json::to_value(value)?;
        self.cset_generic(key, value, version).await
    }

    #[instrument(skip(self), err)]
    pub async fn cset_generic_async(
        &self,
        key: Key,
        value: Value,
        version: CasVersion,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::CSetAsync(key, value, version, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn cset_async<T: Serialize>(
        &self,
        key: Key,
        value: &T,
        version: CasVersion,
    ) -> ConnectionResult<TransactionId> {
        let value = serde_json::to_value(value)?;
        self.cset_generic_async(key, value, version).await
    }

    #[instrument(skip(self), err)]
    pub async fn spub_init(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::SPubInit(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let res = rx.await??;
        Ok(res.transaction_id)
    }

    #[instrument(skip(self), err)]
    pub async fn spub_init_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::SPubInitAsync(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let transaction_id = rx.await?;
        Ok(transaction_id)
    }

    #[instrument(skip(self), err)]
    pub async fn spub_generic(
        &self,
        transaction_id: TransactionId,
        value: Value,
    ) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::SPub(transaction_id, value, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn spub<T: Serialize>(
        &self,
        transaction_id: TransactionId,
        value: &T,
    ) -> ConnectionResult<()> {
        let value = serde_json::to_value(value)?;
        self.spub_generic(transaction_id, value).await
    }

    #[instrument(skip(self), err)]
    pub async fn spub_generic_async(
        &self,
        transaction_id: TransactionId,
        value: Value,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::SPubAsync(transaction_id, value, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn spub_async<T: Serialize>(
        &self,
        transaction_id: TransactionId,
        value: &T,
    ) -> ConnectionResult<TransactionId> {
        let value = serde_json::to_value(value)?;
        self.spub_generic_async(transaction_id, value).await
    }

    #[instrument(skip(self), err)]
    pub async fn publish_generic(&self, key: Key, value: Value) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Publish(key, value, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn publish<T: Serialize>(&self, key: Key, value: &T) -> ConnectionResult<()> {
        let value = json::to_value(value)?;
        self.publish_generic(key, value).await
    }

    #[instrument(skip(self), err)]
    pub async fn publish_generic_async(
        &self,
        key: Key,
        value: Value,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PublishAsync(key, value, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self, value), fields(value), err)]
    pub async fn publish_async<T: Serialize>(
        &self,
        key: Key,
        value: &T,
    ) -> ConnectionResult<TransactionId> {
        let value = json::to_value(value)?;
        self.publish_generic_async(key, value).await
    }

    #[instrument(skip(self), err)]
    pub async fn get_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::GetAsync(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self), ret, err)]
    pub async fn get_generic(&self, key: Key) -> ConnectionResult<Option<Value>> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Get(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        match rx.await? {
            Ok(state) => {
                if let StateEvent::Value(val) = state.event {
                    Ok(Some(val))
                } else {
                    Ok(None)
                }
            }
            Result::Err(e) => {
                if e.error_code == ErrorCode::NoSuchValue {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    #[instrument(skip(self), err)]
    pub async fn get<T: DeserializeOwned>(&self, key: Key) -> ConnectionResult<Option<T>> {
        if let Some(val) = self.get_generic(key).await? {
            Ok(Some(json::from_value(val)?))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self), err)]
    pub async fn cget_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::CGetAsync(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self), ret, err)]
    pub async fn cget_generic(&self, key: Key) -> ConnectionResult<Option<(Value, CasVersion)>> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::CGet(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        match rx.await? {
            Ok(state) => Ok(Some((state.event.value, state.event.version))),
            Result::Err(e) => {
                if e.error_code == ErrorCode::NoSuchValue {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    #[instrument(skip(self), err)]
    pub async fn cget<T: DeserializeOwned>(
        &self,
        key: Key,
    ) -> ConnectionResult<Option<(T, CasVersion)>> {
        if let Some((val, version)) = self.cget_generic(key).await? {
            Ok(Some((json::from_value(val)?, version)))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self), err)]
    pub async fn pget_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PGetAsync(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    #[instrument(skip(self), ret, err)]
    pub async fn pget_generic(&self, key: Key) -> ConnectionResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PGet(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        match rx.await??.event {
            PStateEvent::KeyValuePairs(kvps) => Ok(kvps),
            PStateEvent::Deleted(_) => Ok(vec![]),
        }
    }

    #[instrument(skip(self), err)]
    pub async fn pget<T: DeserializeOwned + Debug>(
        &self,
        key: Key,
    ) -> ConnectionResult<TypedKeyValuePairs<T>> {
        let kvps = self.pget_generic(key).await?;
        let typed_kvps = deserialize_key_value_pairs(kvps)?;
        Ok(typed_kvps)
    }

    #[instrument(skip(self), err)]
    pub async fn delete_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::DeleteAsync(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    #[instrument(skip(self), ret, err)]
    pub async fn delete_generic(&self, key: Key) -> ConnectionResult<Option<Value>> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Delete(key, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        match rx.await? {
            Ok(state) => match state.event {
                StateEvent::Value(_) => Ok(None),
                StateEvent::Deleted(value) => Ok(Some(value)),
            },
            Result::Err(e) => {
                if e.error_code == ErrorCode::NoSuchValue {
                    Ok(None)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    #[instrument(skip(self), err)]
    pub async fn delete<T: DeserializeOwned + Debug>(
        &self,
        key: Key,
    ) -> ConnectionResult<Option<T>> {
        if let Some(val) = self.delete_generic(key).await? {
            Ok(Some(json::from_value(val)?))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self), err)]
    pub async fn pdelete_async(&self, key: Key, quiet: bool) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PDeleteAsync(key, quiet, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    #[instrument(skip(self), ret, err)]
    pub async fn pdelete_generic(&self, key: Key, quiet: bool) -> ConnectionResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PDelete(key, quiet, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        match rx.await??.event {
            PStateEvent::KeyValuePairs(_) => Ok(vec![]),
            PStateEvent::Deleted(kvps) => Ok(kvps),
        }
    }

    #[instrument(skip(self), err)]
    pub async fn pdelete<T: DeserializeOwned + Debug>(
        &self,
        key: Key,
        quiet: bool,
    ) -> ConnectionResult<TypedKeyValuePairs<T>> {
        let kvps = self.pdelete_generic(key, quiet).await?;
        let typed_kvps = deserialize_key_value_pairs(kvps)?;
        Ok(typed_kvps)
    }

    #[instrument(skip(self), err)]
    pub async fn ls_async(&self, parent: Option<Key>) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::LsAsync(parent, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    #[instrument(skip(self), ret, err)]
    pub async fn ls(&self, parent: Option<Key>) -> ConnectionResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::Ls(parent, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let children = rx.await??.children;
        Ok(children)
    }

    #[instrument(skip(self), err)]
    pub async fn pls_async(
        &self,
        parent: Option<RequestPattern>,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PLsAsync(parent, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let tid = rx.await?;
        Ok(tid)
    }

    #[instrument(skip(self), ret, err)]
    pub async fn pls(
        &self,
        parent: Option<RequestPattern>,
    ) -> ConnectionResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        let cmd = Command::PLs(parent, tx);
        debug!("Queuing command {cmd:?}");
        self.commands.send(cmd).await?;
        debug!("Command queued.");
        let children = rx.await??.children;
        Ok(children)
    }

    #[instrument(skip(self), err)]
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

    #[instrument(skip(self), err)]
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
        let res = tid_rx.await??;
        Ok((val_rx, res.transaction_id))
    }

    #[instrument(skip(self), err)]
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

    #[instrument(skip(self), err)]
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

    #[instrument(skip(self), err)]
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
        let res = tid_rx.await??;
        Ok((event_rx, res.transaction_id))
    }

    #[instrument(skip(self), err)]
    pub async fn psubscribe<T: DeserializeOwned + Debug + Send + 'static>(
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

    #[instrument(skip(self), err)]
    pub async fn unsubscribe(&self, transaction_id: TransactionId) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Unsubscribe(transaction_id, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn unsubscribe_async(
        &self,
        transaction_id: TransactionId,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::UnsubscribeAsync(transaction_id, tx))
            .await?;
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub async fn subscribe_ls_async(&self, parent: Option<Key>) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::SubscribeLsAsync(parent, tx))
            .await?;
        let tid = rx.await?;
        Ok(tid)
    }

    #[instrument(skip(self), err)]
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
        let res = tid_rx.await??;
        Ok((children_rx, res.transaction_id))
    }

    #[instrument(skip(self), err)]
    pub async fn unsubscribe_ls(&self, transaction_id: TransactionId) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::UnsubscribeLs(transaction_id, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn unsubscribe_ls_async(
        &self,
        transaction_id: TransactionId,
    ) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::UnsubscribeLsAsync(transaction_id, tx))
            .await?;
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub async fn lock(&self, key: Key) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::Lock(key, tx)).await?;
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn lock_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::LockAsync(key, tx)).await?;
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub async fn acquire_lock(&self, key: Key) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::AcquireLock(key, tx)).await?;
        match rx.await? {
            Ok(_) => Ok(()),
            Result::Err(e) => Err(ConnectionError::ServerResponse(Box::new(e))),
        }
    }

    #[instrument(skip(self), err)]
    pub async fn release_lock(&self, key: Key) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::ReleaseLock(key, tx)).await?;
        rx.await??;
        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn release_lock_async(&self, key: Key) -> ConnectionResult<TransactionId> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::ReleaseLockAsync(key, tx))
            .await?;
        let res = rx.await?;
        Ok(res)
    }

    #[instrument(skip(self, seed, update), err)]
    pub async fn update<T: DeserializeOwned + Serialize + Debug, S: Fn() -> T, F: Fn(&mut T)>(
        &self,
        key: Key,
        seed: S,
        update: F,
    ) -> ConnectionResult<()> {
        let f = |mut set: Option<T>| match set.take() {
            Some(mut it) => {
                update(&mut it);
                it
            }
            None => {
                let mut it = seed();
                update(&mut it);
                it
            }
        };

        self.try_update(key, f, 0).await
    }

    #[instrument(skip(self, swap), err)]
    pub async fn swap<T: DeserializeOwned + Debug, V: Serialize, F: Fn(Option<T>) -> V>(
        &self,
        key: Key,
        swap: F,
    ) -> ConnectionResult<()> {
        self.try_update(key, swap, 0).await
    }

    #[instrument(skip(self, transform), level = "debug", err)]
    async fn try_update<T: DeserializeOwned + Debug, V: Serialize, F: Fn(Option<T>) -> V>(
        &self,
        key: Key,
        transform: F,
        counter: usize,
    ) -> ConnectionResult<()> {
        if counter >= 100 {
            return Err(ConnectionError::Timeout(Box::new(
                "could not update, value keeps being changed by another instance".to_owned(),
            )));
        }

        let (new_val, version) = match self.cget::<T>(key.clone()).await? {
            Some((val, version)) => (transform(Some(val)), version),
            None => (transform(None), 0),
        };

        if let Err(e) = self.cset(key.clone(), new_val, version).await {
            match e {
                ConnectionError::ServerResponse(_) => {
                    tracing::debug!(
                        "value has changed in the mean time, re-fetching and trying again"
                    );
                    Box::pin(self.try_update(key, transform, counter + 1)).await
                }
                e => Err(e),
            }
        } else {
            Ok(())
        }
    }

    #[instrument(skip(self))]
    pub async fn send_buffer(&self, delay: Duration) -> SendBuffer {
        SendBuffer::new(self.commands.clone(), delay).await
    }

    #[instrument(skip(self), err)]
    pub async fn close(&self) -> ConnectionResult<()> {
        let (tx, rx) = oneshot::channel();
        self.stop.send(tx).await?;
        rx.await.ok();
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn all_messages(&self) -> ConnectionResult<mpsc::UnboundedReceiver<ServerMessage>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.commands.send(Command::AllMessages(tx)).await?;
        Ok(rx)
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

#[instrument(level = "trace")]
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
                    error!("could not deserialize json value to requested type: {e}");
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

#[instrument(level = "trace")]
async fn deserialize_events<T: DeserializeOwned + Debug + Send + 'static>(
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
                error!("could not deserialize json to requested type: {e}");
                break;
            }
        }
    }
}

type GenericCallback = mpsc::UnboundedSender<ServerMessage>;
type AsyncTicket = oneshot::Sender<TransactionId>;
type AckCallback = oneshot::Sender<Result<Ack, Err>>;
type StateCallback = oneshot::Sender<Result<State, Err>>;
type CStateCallback = oneshot::Sender<Result<CState, Err>>;
type PStateCallback = oneshot::Sender<Result<PState, Err>>;
type LsStateCallback = oneshot::Sender<Result<LsState, Err>>;

type GenericCallbacks = Vec<GenericCallback>;
type AckCallbacks = HashMap<TransactionId, AckCallback>;
type StateCallbacks = HashMap<TransactionId, StateCallback>;
type CStateCallbacks = HashMap<TransactionId, CStateCallback>;
type PStateCallbacks = HashMap<TransactionId, PStateCallback>;
type LsStateCallbacks = HashMap<TransactionId, LsStateCallback>;
type SubCallbacks = HashMap<TransactionId, mpsc::UnboundedSender<Option<Value>>>;
type PSubCallbacks = HashMap<TransactionId, mpsc::UnboundedSender<PStateEvent>>;
type SubLsCallbacks = HashMap<TransactionId, mpsc::UnboundedSender<Vec<RegularKeySegment>>>;

#[derive(Default)]
struct Callbacks {
    generic: GenericCallbacks,
    ack: AckCallbacks,
    state: StateCallbacks,
    cstate: CStateCallbacks,
    pstate: PStateCallbacks,
    lsstate: LsStateCallbacks,
    sub: SubCallbacks,
    psub: PSubCallbacks,
    subls: SubLsCallbacks,
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

#[instrument(, err)]
pub async fn connect_with_default_config() -> ConnectionResult<(Worterbuch, OnDisconnect, Config)> {
    let config = Config::new();
    let (conn, disconnected) = connect(config.clone()).await?;
    Ok((conn, disconnected, config))
}

#[instrument(, err)]
pub async fn connect(config: Config) -> ConnectionResult<(Worterbuch, OnDisconnect)> {
    let mut err = None;
    for addr in &config.servers {
        info!("Trying to connect to server {addr} …");
        match try_connect(config.clone(), *addr).await {
            Ok(con) => {
                info!("Successfully connected to server {addr}");
                return Ok(con);
            }
            Err(e) => {
                warn!("Could not connect to server {addr}: {e}");
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

#[instrument(skip(config), err(level = Level::WARN))]
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

    debug!("Got server url from config: {url}");

    let (disco_tx, disco_rx) = oneshot::channel();

    let wb = if tcp {
        #[cfg(not(feature = "tcp"))]
        panic!("tcp not supported, binary was compiled without the tcp feature flag");
        #[cfg(feature = "tcp")]
        connect_tcp(host_addr, disco_tx, config).await?
    } else if unix {
        #[cfg(not(all(target_family = "unix", feature = "unix")))]
        panic!(
            "not supported, binary was compile without the unix feature flag or for non-unix operating systems"
        );
        #[cfg(all(target_family = "unix", feature = "unix"))]
        connect_unix(url, disco_tx, config).await?
    } else {
        #[cfg(not(any(feature = "ws", feature = "wasm")))]
        panic!("websocket not supported, binary was compiled without the ws feature flag");
        #[cfg(any(feature = "ws", feature = "wasm"))]
        connect_ws(url, disco_tx, config).await?
    };

    let disconnected = OnDisconnect { rx: disco_rx };

    Ok((wb, disconnected))
}

#[cfg(any(feature = "ws", feature = "wasm"))]
#[instrument(skip(config, on_disconnect), level = Level::INFO)]
async fn connect_ws(
    url: String,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    debug!("Connecting to server {url} over websocket …");

    #[cfg(feature = "ws")]
    let mut websocket = {
        let auth_token = config.auth_token.clone();
        let mut request = Request::builder()
            .uri(url)
            .header("Sec-WebSocket-Protocol", "worterbuch".to_owned())
            .header("Sec-WebSocket-Key", generate_key());

        if let Some(auth_token) = auth_token {
            request = request.header("Authorization", format!("Bearer {auth_token}"));
        }
        let request: Request<()> = request.body(())?;

        #[cfg(feature = "ws")]
        let (websocket, _) = connect_async_with_config(request, None, true).await?;

        websocket
    };

    #[cfg(feature = "wasm")]
    let mut websocket = connect_wasm(url).await?;

    debug!("Connected to server.");

    let Welcome { client_id, info } = match websocket.next().await {
        Some(Ok(msg)) => match msg.to_text() {
            Ok(data) => match json::from_str::<SM>(data) {
                Ok(SM::Welcome(welcome)) => {
                    debug!("Welcome message received: {welcome:?}");
                    welcome
                }
                Ok(msg) => {
                    return Err(ConnectionError::IoError(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("server sent invalid welcome message: {msg:?}"),
                    ))));
                }
                Err(e) => {
                    return Err(ConnectionError::IoError(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("error parsing welcome message '{data}': {e}"),
                    ))));
                }
            },
            Err(e) => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid welcome message '{msg:?}': {e}"),
                ))));
            }
        },
        Some(Err(e)) => return Err(e.into()),
        None => {
            return Err(ConnectionError::IoError(Box::new(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "connection closed before welcome message",
            ))));
        }
    };

    let proto_version = if let Some(v) = info
        .supported_protocol_versions
        .iter()
        .find(|v| PROTOCOL_VERSION.is_compatible_with_server(v))
    {
        v
    } else {
        return Err(ConnectionError::WorterbuchError(Box::new(
            WorterbuchError::ProtocolNegotiationFailed(PROTOCOL_VERSION.major()),
        )));
    };

    debug!("Found compatible protocol version {proto_version}.");

    let proto_switch = ProtocolSwitchRequest {
        version: proto_version.major(),
    };
    let msg = json::to_string(&CM::ProtocolSwitchRequest(proto_switch))?;
    debug!("Sending protocol switch message: {msg}");
    websocket.send(Message::Text(msg.into())).await?;

    match websocket.next().await {
        Some(msg) => match msg? {
            Message::Text(msg) => match serde_json::from_str(&msg) {
                Ok(SM::Ack(_)) => {
                    debug!("Protocol switched to v{}.", proto_version.major());
                }
                Ok(SM::Err(e)) => {
                    error!("Protocol switch failed: {e}");
                    return Err(ConnectionError::WorterbuchError(Box::new(
                        WorterbuchError::ServerResponse(e),
                    )));
                }
                Ok(msg) => {
                    return Err(ConnectionError::IoError(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("server sent invalid protocol switch response: {msg:?}"),
                    ))));
                }
                Err(e) => {
                    return Err(ConnectionError::IoError(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("error receiving protocol switch response: {e}"),
                    ))));
                }
            },
            msg => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("received unexpected message from server: {msg:?}"),
                ))));
            }
        },
        None => {
            warn!("Server closed the connection");
            return Err(ConnectionError::IoError(Box::new(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection closed before welcome message",
            ))));
        }
    }

    if info.authorization_required {
        if let Some(auth_token) = config.auth_token.clone() {
            let handshake = AuthorizationRequest { auth_token };
            let msg = json::to_string(&CM::AuthorizationRequest(handshake))?;
            debug!("Sending authorization message: {msg}");
            websocket.send(Message::Text(msg.into())).await?;

            match websocket.next().await {
                Some(Err(e)) => Err(e.into()),
                Some(Ok(Message::Text(msg))) => match serde_json::from_str(&msg) {
                    Ok(SM::Authorized(_)) => {
                        debug!("Authorization accepted.");
                        connected(
                            ClientSocket::Ws(WsClientSocket::new(websocket)),
                            on_disconnect,
                            config,
                            client_id,
                        )
                    }
                    Ok(SM::Err(e)) => {
                        error!("Authorization failed: {e}");
                        Err(ConnectionError::WorterbuchError(Box::new(
                            WorterbuchError::ServerResponse(e),
                        )))
                    }
                    Ok(msg) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("server sent invalid authetication response: {msg:?}"),
                    )))),
                    Err(e) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("error receiving authorization response: {e}"),
                    )))),
                },
                Some(Ok(msg)) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("received unexpected message from server: {msg:?}"),
                )))),
                None => Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before welcome message",
                )))),
            }
        } else {
            Err(ConnectionError::AuthorizationError(Box::new(
                "Server requires authorization but no auth token was provided.".to_owned(),
            )))
        }
    } else {
        connected(
            ClientSocket::Ws(WsClientSocket::new(websocket)),
            on_disconnect,
            config,
            client_id,
        )
    }
}

#[cfg(feature = "tcp")]
#[instrument(skip(config, on_disconnect))]
async fn connect_tcp(
    host_addr: SocketAddr,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    let timeout = config.connection_timeout;
    debug!(
        "Connecting to server tcp://{host_addr} (timeout: {} ms) …",
        timeout.as_millis()
    );

    let stream = select! {
        conn = TcpStream::connect(host_addr) => conn,
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout(Box::new("Timeout while waiting for TCP connection.".to_owned())));
        },
    }?;
    debug!("Connected to tcp://{host_addr}.");
    let (tcp_rx, mut tcp_tx) = stream.into_split();
    let mut tcp_rx = BufReader::new(tcp_rx).lines();

    debug!("Connected to server.");

    let Welcome { client_id, info } = select! {
        line = tcp_rx.next_line() => match line {
            Ok(None) => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before welcome message",
                ))))
            }
            Ok(Some(line)) => {
                let msg = json::from_str::<SM>(&line);
                match msg {
                    Ok(SM::Welcome(welcome)) => {
                        debug!("Welcome message received: {welcome:?}");
                        welcome
                    }
                    Ok(msg) => {
                        return Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid welcome message: {msg:?}"),
                        ))))
                    }
                    Err(e) => {
                        return Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error parsing welcome message '{line}': {e}"),
                        ))))
                    }
                }
            }
            Err(e) => return Err(ConnectionError::IoError(Box::new(e))),
        },
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout(Box::new("Timeout while waiting for welcome message.".to_owned())));
        },
    };

    let proto_version = if let Some(v) = info
        .supported_protocol_versions
        .iter()
        .find(|v| PROTOCOL_VERSION.is_compatible_with_server(v))
    {
        v
    } else {
        return Err(ConnectionError::WorterbuchError(Box::new(
            WorterbuchError::ProtocolNegotiationFailed(PROTOCOL_VERSION.major()),
        )));
    };

    debug!("Found compatible protocol version {proto_version}.");

    let proto_switch = ProtocolSwitchRequest {
        version: proto_version.major(),
    };
    let mut msg = json::to_string(&CM::ProtocolSwitchRequest(proto_switch))?;
    msg.push('\n');
    debug!("Sending protocol switch message: {msg}");
    tcp_tx.write_all(msg.as_bytes()).await?;

    match tcp_rx.next_line().await {
        Ok(None) => {
            return Err(ConnectionError::IoError(Box::new(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection closed before handshake",
            ))));
        }
        Ok(Some(line)) => match serde_json::from_str(&line) {
            Ok(SM::Ack(_)) => {
                debug!("Protocol switched to v{}.", proto_version.major());
            }
            Ok(SM::Err(e)) => {
                error!("Protocol switch failed: {e}");
                return Err(ConnectionError::WorterbuchError(Box::new(
                    WorterbuchError::ServerResponse(e),
                )));
            }
            Ok(msg) => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid protocol switch response: {msg:?}"),
                ))));
            }
            Err(e) => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("error receiving protocol switch response: {e}"),
                ))));
            }
        },
        Err(e) => {
            warn!("Server closed the connection");
            return Err(ConnectionError::IoError(Box::new(e)));
        }
    }

    if info.authorization_required {
        if let Some(auth_token) = config.auth_token.clone() {
            let handshake = AuthorizationRequest { auth_token };
            let mut msg = json::to_string(&CM::AuthorizationRequest(handshake))?;
            msg.push('\n');
            debug!("Sending authorization message: {msg}");
            tcp_tx.write_all(msg.as_bytes()).await?;

            match tcp_rx.next_line().await {
                Ok(None) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before handshake",
                )))),
                Ok(Some(line)) => {
                    let msg = json::from_str::<SM>(&line);
                    match msg {
                        Ok(SM::Authorized(_)) => {
                            debug!("Authorization accepted.");
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
                            )
                        }
                        Ok(SM::Err(e)) => {
                            error!("Authorization failed: {e}");
                            Err(ConnectionError::WorterbuchError(Box::new(
                                WorterbuchError::ServerResponse(e),
                            )))
                        }
                        Ok(msg) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid authetication response: {msg:?}"),
                        )))),
                        Err(e) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error receiving authorization response: {e}"),
                        )))),
                    }
                }
                Err(e) => Err(ConnectionError::IoError(Box::new(e))),
            }
        } else {
            Err(ConnectionError::AuthorizationError(Box::new(
                "Server requires authorization but no auth token was provided.".to_owned(),
            )))
        }
    } else {
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
        )
    }
}

#[cfg(all(target_family = "unix", feature = "unix"))]
#[instrument(skip(config, on_disconnect), err(level = Level::WARN))]
async fn connect_unix(
    path: String,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
) -> Result<Worterbuch, ConnectionError> {
    let timeout = config.connection_timeout;
    debug!(
        "Connecting to server socket {path} (timeout: {} ms) …",
        timeout.as_millis()
    );

    let stream = select! {
        conn = UnixStream::connect(&path) => conn,
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout(Box::new("Timeout while waiting for TCP connection.".to_owned())));
        },
    }?;
    debug!("Connected to {path}.");
    let (tcp_rx, mut tcp_tx) = stream.into_split();
    let mut tcp_rx = BufReader::new(tcp_rx).lines();

    debug!("Connected to server.");

    let Welcome { client_id, info } = select! {
        line = tcp_rx.next_line() => match line {
            Ok(None) => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before welcome message",
                ))))
            }
            Ok(Some(line)) => {
                let msg = json::from_str::<SM>(&line);
                match msg {
                    Ok(SM::Welcome(welcome)) => {
                        debug!("Welcome message received: {welcome:?}");
                        welcome
                    }
                    Ok(msg) => {
                        return Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid welcome message: {msg:?}"),
                        ))))
                    }
                    Err(e) => {
                        return Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error parsing welcome message '{line}': {e}"),
                        ))))
                    }
                }
            }
            Err(e) => return Err(ConnectionError::IoError(Box::new(e))),
        },
        _ = sleep(timeout) => {
            return Err(ConnectionError::Timeout(Box::new("Timeout while waiting for welcome message.".to_owned())));
        },
    };

    let proto_version = if let Some(v) = info
        .supported_protocol_versions
        .iter()
        .find(|v| PROTOCOL_VERSION.is_compatible_with_server(v))
    {
        v
    } else {
        return Err(ConnectionError::WorterbuchError(Box::new(
            WorterbuchError::ProtocolNegotiationFailed(PROTOCOL_VERSION.major()),
        )));
    };

    debug!("Found compatible protocol version {proto_version}.");

    let proto_switch = ProtocolSwitchRequest {
        version: proto_version.major(),
    };
    let mut msg = json::to_string(&CM::ProtocolSwitchRequest(proto_switch))?;
    msg.push('\n');
    debug!("Sending protocol switch message: {msg}");
    tcp_tx.write_all(msg.as_bytes()).await?;

    match tcp_rx.next_line().await {
        Ok(None) => {
            return Err(ConnectionError::IoError(Box::new(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "connection closed before handshake",
            ))));
        }
        Ok(Some(line)) => match serde_json::from_str(&line) {
            Ok(SM::Ack(_)) => {
                debug!("Protocol switched to v{}.", proto_version.major());
            }
            Ok(SM::Err(e)) => {
                error!("Protocol switch failed: {e}");
                return Err(ConnectionError::WorterbuchError(Box::new(
                    WorterbuchError::ServerResponse(e),
                )));
            }
            Ok(msg) => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("server sent invalid protocol switch response: {msg:?}"),
                ))));
            }
            Err(e) => {
                return Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("error receiving protocol switch response: {e}"),
                ))));
            }
        },
        Err(e) => {
            warn!("Server closed the connection");
            return Err(ConnectionError::IoError(Box::new(e)));
        }
    }

    if info.authorization_required {
        if let Some(auth_token) = config.auth_token.clone() {
            let handshake = AuthorizationRequest { auth_token };
            let mut msg = json::to_string(&CM::AuthorizationRequest(handshake))?;
            msg.push('\n');
            debug!("Sending authorization message: {msg}");
            tcp_tx.write_all(msg.as_bytes()).await?;

            match tcp_rx.next_line().await {
                Ok(None) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "connection closed before handshake",
                )))),
                Ok(Some(line)) => {
                    let msg = json::from_str::<SM>(&line);
                    match msg {
                        Ok(SM::Authorized(_)) => {
                            debug!("Authorization accepted.");
                            connected(
                                ClientSocket::Unix(
                                    UnixClientSocket::new(
                                        tcp_tx,
                                        tcp_rx,
                                        config.channel_buffer_size,
                                    )
                                    .await,
                                ),
                                on_disconnect,
                                config,
                                client_id,
                            )
                        }
                        Ok(SM::Err(e)) => {
                            error!("Authorization failed: {e}");
                            Err(ConnectionError::WorterbuchError(Box::new(
                                WorterbuchError::ServerResponse(e),
                            )))
                        }
                        Ok(msg) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("server sent invalid authetication response: {msg:?}"),
                        )))),
                        Err(e) => Err(ConnectionError::IoError(Box::new(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("error receiving authorization response: {e}"),
                        )))),
                    }
                }
                Err(e) => Err(ConnectionError::IoError(Box::new(e))),
            }
        } else {
            Err(ConnectionError::AuthorizationError(Box::new(
                "Server requires authorization but no auth token was provided.".to_owned(),
            )))
        }
    } else {
        connected(
            ClientSocket::Unix(
                UnixClientSocket::new(tcp_tx, tcp_rx, config.channel_buffer_size).await,
            ),
            on_disconnect,
            config,
            client_id,
        )
    }
}

#[instrument(skip(client_socket, on_disconnect, config))]
fn connected(
    client_socket: ClientSocket,
    on_disconnect: oneshot::Sender<()>,
    config: Config,
    client_id: String,
) -> Result<Worterbuch, ConnectionError> {
    let (stop_tx, stop_rx) = mpsc::channel(1);
    let (cmd_tx, cmd_rx) = mpsc::channel(1);

    spawn(async move {
        if let Err(e) = run(cmd_rx, client_socket, stop_rx, config).await {
            error!("Connection closed with error: {e}");
        } else {
            debug!("Connection closed.");
        }
        on_disconnect.send(()).ok();
    });

    Ok(Worterbuch::new(cmd_tx, stop_tx, client_id))
}

#[instrument(skip(cmd_rx, client_socket, stop_rx, config), err)]
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
        trace!("loop: wait for command / ws message / shutdown request");
        select! {
            recv = stop_rx.recv() => {
                debug!("Shutdown request received.");
                stop_tx = recv;
                break;
            },
            ws_msg = client_socket.receive_msg() => {
                match process_incoming_server_message(ws_msg, &mut callbacks).await {
                    Ok(ControlFlow::Break(_)) => break,
                    Err(e) => {
                        error!("Error processing server message: {e}");
                        break;
                    },
                    _ => trace!("websocket message processing done")
                }
            },
            cmd = cmd_rx.recv() => {
                match process_incoming_command(cmd, &mut callbacks, &mut transaction_ids).await {
                    Ok(ControlFlow::Continue(msg)) => if let Some(msg) = msg
                        && let Err(e) = client_socket.send_msg(msg, config.use_backpressure).await {
                                error!("Error sending message to server: {e}");
                                break;
                            },
                    Ok(ControlFlow::Break(_)) => break,
                    Err(e) => {
                        error!("Error processing command: {e}");
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

#[instrument(skip(callbacks, transaction_ids), level = "trace", err)]
async fn process_incoming_command(
    cmd: Option<Command>,
    callbacks: &mut Callbacks,
    transaction_ids: &mut TransactionIds,
) -> ConnectionResult<ControlFlow<(), Option<CM>>> {
    if let Some(command) = cmd {
        debug!("Processing command: {command:?}");
        let transaction_id = transaction_ids.next();
        let cm = match command {
            Command::Set(key, value, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::Set(Set {
                    transaction_id,
                    key,
                    value,
                }))
            }
            Command::SetAsync(key, value, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::Set(Set {
                    transaction_id,
                    key,
                    value,
                }))
            }
            Command::CSet(key, value, version, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::CSet(CSet {
                    transaction_id,
                    key,
                    value,
                    version,
                }))
            }
            Command::CSetAsync(key, value, version, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::CSet(CSet {
                    transaction_id,
                    key,
                    value,
                    version,
                }))
            }
            Command::SPubInit(key, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::SPubInit(SPubInit {
                    transaction_id,
                    key,
                }))
            }
            Command::SPubInitAsync(key, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::SPubInit(SPubInit {
                    transaction_id,
                    key,
                }))
            }
            Command::SPub(transaction_id, value, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::SPub(SPub {
                    transaction_id,
                    value,
                }))
            }
            Command::SPubAsync(transaction_id, value, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::SPub(SPub {
                    transaction_id,
                    value,
                }))
            }
            Command::Publish(key, value, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::Publish(Publish {
                    transaction_id,
                    key,
                    value,
                }))
            }
            Command::PublishAsync(key, value, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::Publish(Publish {
                    transaction_id,
                    key,
                    value,
                }))
            }
            Command::Get(key, callback) => {
                callbacks.state.insert(transaction_id, callback);
                Some(CM::Get(Get {
                    transaction_id,
                    key,
                }))
            }
            Command::GetAsync(key, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::Get(Get {
                    transaction_id,
                    key,
                }))
            }
            Command::CGet(key, callback) => {
                callbacks.cstate.insert(transaction_id, callback);
                Some(CM::CGet(Get {
                    transaction_id,
                    key,
                }))
            }
            Command::CGetAsync(key, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::CGet(Get {
                    transaction_id,
                    key,
                }))
            }
            Command::PGet(request_pattern, callback) => {
                callbacks.pstate.insert(transaction_id, callback);
                Some(CM::PGet(PGet {
                    transaction_id,
                    request_pattern,
                }))
            }
            Command::PGetAsync(request_pattern, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::PGet(PGet {
                    transaction_id,
                    request_pattern,
                }))
            }
            Command::Delete(key, callback) => {
                callbacks.state.insert(transaction_id, callback);
                Some(CM::Delete(Delete {
                    transaction_id,
                    key,
                }))
            }
            Command::DeleteAsync(key, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::Delete(Delete {
                    transaction_id,
                    key,
                }))
            }
            Command::PDelete(request_pattern, quiet, callback) => {
                callbacks.pstate.insert(transaction_id, callback);
                Some(CM::PDelete(PDelete {
                    transaction_id,
                    request_pattern,
                    quiet: Some(quiet),
                }))
            }
            Command::PDeleteAsync(request_pattern, quiet, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::PDelete(PDelete {
                    transaction_id,
                    request_pattern,
                    quiet: Some(quiet),
                }))
            }
            Command::Ls(parent, callback) => {
                callbacks.lsstate.insert(transaction_id, callback);
                Some(CM::Ls(Ls {
                    transaction_id,
                    parent,
                }))
            }
            Command::LsAsync(parent, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::Ls(Ls {
                    transaction_id,
                    parent,
                }))
            }
            Command::PLs(parent_pattern, callback) => {
                callbacks.lsstate.insert(transaction_id, callback);
                Some(CM::PLs(PLs {
                    transaction_id,
                    parent_pattern,
                }))
            }
            Command::PLsAsync(parent_pattern, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::PLs(PLs {
                    transaction_id,
                    parent_pattern,
                }))
            }
            Command::Subscribe(key, unique, tid_callback, value_callback, live_only) => {
                callbacks.sub.insert(transaction_id, value_callback);
                callbacks.ack.insert(transaction_id, tid_callback);
                Some(CM::Subscribe(Subscribe {
                    transaction_id,
                    key,
                    unique,
                    live_only: Some(live_only),
                }))
            }
            Command::SubscribeAsync(key, unique, callback, live_only) => {
                callback.send(transaction_id).ok();
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
                callbacks.ack.insert(transaction_id, tid_callback);
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
                callback.send(transaction_id).ok();
                Some(CM::PSubscribe(PSubscribe {
                    transaction_id,
                    request_pattern,
                    unique,
                    aggregate_events,
                    live_only: Some(live_only),
                }))
            }
            Command::Unsubscribe(transaction_id, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                callbacks.sub.remove(&transaction_id);
                callbacks.psub.remove(&transaction_id);
                Some(CM::Unsubscribe(Unsubscribe { transaction_id }))
            }
            Command::UnsubscribeAsync(transaction_id, callback) => {
                callbacks.sub.remove(&transaction_id);
                callbacks.psub.remove(&transaction_id);
                callback.send(transaction_id).ok();
                Some(CM::Unsubscribe(Unsubscribe { transaction_id }))
            }
            Command::SubscribeLs(parent, tid_callback, children_callback) => {
                callbacks.subls.insert(transaction_id, children_callback);
                callbacks.ack.insert(transaction_id, tid_callback);
                Some(CM::SubscribeLs(SubscribeLs {
                    transaction_id,
                    parent,
                }))
            }
            Command::SubscribeLsAsync(parent, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::SubscribeLs(SubscribeLs {
                    transaction_id,
                    parent,
                }))
            }
            Command::UnsubscribeLs(transaction_id, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                callbacks.subls.remove(&transaction_id);
                Some(CM::UnsubscribeLs(UnsubscribeLs { transaction_id }))
            }
            Command::UnsubscribeLsAsync(transaction_id, callback) => {
                callbacks.subls.remove(&transaction_id);
                callback.send(transaction_id).ok();
                Some(CM::Unsubscribe(Unsubscribe { transaction_id }))
            }
            Command::Lock(key, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::Lock(Lock {
                    transaction_id,
                    key,
                }))
            }
            Command::LockAsync(key, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::Lock(Lock {
                    transaction_id,
                    key,
                }))
            }
            Command::AcquireLock(key, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::AcquireLock(Lock {
                    transaction_id,
                    key,
                }))
            }
            Command::ReleaseLock(key, callback) => {
                callbacks.ack.insert(transaction_id, callback);
                Some(CM::ReleaseLock(Lock {
                    transaction_id,
                    key,
                }))
            }
            Command::ReleaseLockAsync(key, callback) => {
                callback.send(transaction_id).ok();
                Some(CM::ReleaseLock(Lock {
                    transaction_id,
                    key,
                }))
            }
            Command::AllMessages(tx) => {
                callbacks.generic.push(tx);
                None
            }
        };
        Ok(ControlFlow::Continue(cm))
    } else {
        debug!("No more commands");
        Ok(ControlFlow::Break(()))
    }
}

#[instrument(skip(callbacks), level = "trace", err)]
async fn process_incoming_server_message(
    msg: ConnectionResult<Option<ServerMessage>>,
    callbacks: &mut Callbacks,
) -> ConnectionResult<ControlFlow<()>> {
    match msg {
        Ok(Some(msg)) => {
            deliver_generic(&msg, callbacks);
            match msg {
                SM::State(state) => deliver_state(state, callbacks).await?,
                SM::CState(state) => deliver_cstate(state, callbacks).await?,
                SM::PState(pstate) => deliver_pstate(pstate, callbacks).await?,
                SM::LsState(ls) => deliver_ls(ls, callbacks).await?,
                SM::Err(err) => deliver_err(err, callbacks).await,
                SM::Ack(ack) => deliver_ack(ack, callbacks).await,
                SM::Welcome(_) | SM::Authorized(_) => (),
            }
            Ok(ControlFlow::Continue(()))
        }
        Ok(None) => {
            warn!("Connection closed.");
            Ok(ControlFlow::Break(()))
        }
        Err(e) => {
            error!("Error receiving message: {e}");
            Ok(ControlFlow::Break(()))
        }
    }
}

#[instrument(skip(callbacks), level = "trace", ret)]
fn deliver_generic(msg: &ServerMessage, callbacks: &mut Callbacks) {
    callbacks.generic.retain(|tx| match tx.send(msg.clone()) {
        Ok(_) => true,
        Err(e) => {
            error!("Removing callback due to failure to deliver message to receiver: {e}");
            false
        }
    });
}

#[instrument(skip(callbacks), level = "trace", err)]
async fn deliver_state(state: State, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.state.remove(&state.transaction_id) {
        cb.send(Ok(state.clone())).ok();
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

#[instrument(skip(callbacks), level = "trace", err)]
async fn deliver_cstate(state: CState, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.cstate.remove(&state.transaction_id) {
        cb.send(Ok(state)).ok();
    }
    Ok(())
}

#[instrument(skip(callbacks), level = "trace", err)]
async fn deliver_pstate(pstate: PState, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.pstate.remove(&pstate.transaction_id) {
        cb.send(Ok(pstate.clone())).ok();
    }

    if let Some(cb) = callbacks.psub.get(&pstate.transaction_id) {
        cb.send(pstate.event)?;
    }
    Ok(())
}

#[instrument(skip(callbacks), level = "trace", err)]
async fn deliver_ls(ls: LsState, callbacks: &mut Callbacks) -> ConnectionResult<()> {
    if let Some(cb) = callbacks.lsstate.remove(&ls.transaction_id) {
        cb.send(Ok(ls.clone())).ok();
    }

    if let Some(cb) = callbacks.subls.get(&ls.transaction_id) {
        cb.send(ls.children)?;
    }

    Ok(())
}

#[instrument(skip(callbacks), level = "trace", ret)]
async fn deliver_ack(ack: Ack, callbacks: &mut Callbacks) {
    if let Some(cb) = callbacks.ack.remove(&ack.transaction_id) {
        cb.send(Ok(ack)).ok();
    }
}

#[instrument(skip(callbacks), level = "trace", ret)]
async fn deliver_err(err: Err, callbacks: &mut Callbacks) {
    if let Some(cb) = callbacks.ack.remove(&err.transaction_id) {
        cb.send(Err(err.clone())).ok();
    }
    if let Some(cb) = callbacks.state.remove(&err.transaction_id) {
        cb.send(Err(err.clone())).ok();
    }
    if let Some(cb) = callbacks.cstate.remove(&err.transaction_id) {
        cb.send(Err(err.clone())).ok();
    }
    if let Some(cb) = callbacks.pstate.remove(&err.transaction_id) {
        cb.send(Err(err.clone())).ok();
    }
    if let Some(cb) = callbacks.lsstate.remove(&err.transaction_id) {
        cb.send(Err(err.clone())).ok();
    }
}

#[instrument(level = "trace", err)]
fn deserialize_key_value_pairs<T: DeserializeOwned + Debug>(
    kvps: KeyValuePairs,
) -> Result<TypedKeyValuePairs<T>, ConnectionError> {
    let mut typed = TypedKeyValuePairs::new();
    for kvp in kvps {
        typed.push(kvp.try_into()?);
    }
    Ok(typed)
}

#[instrument(level = "trace", err)]
fn deserialize_pstate_event<T: DeserializeOwned + Debug>(
    pstate: PStateEvent,
) -> Result<TypedPStateEvent<T>, SubscriptionError> {
    Ok(pstate.try_into()?)
}
