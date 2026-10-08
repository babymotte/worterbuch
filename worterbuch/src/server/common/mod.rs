/*
 *  Worterbuch server common module
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

pub mod protocol;

use crate::{Config, cluster::protocol::locks::Locks, stats::VERSION};
use hashbrown::HashMap;
use std::{fmt, net::SocketAddr, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{Level, Span, debug, instrument, trace};
use worterbuch_common::{
    ClientId, LockAcquiredReceiver, LockLostReceiver, LsSubscription, PSubscription, Protocol,
    RegularKeySegment, Subscription, ValueEntry, WbApi,
    error::{WorterbuchError, WorterbuchResult},
    protocol::v1::{
        CasVersion, GraveGoods, Interface, Key, KeyValuePairs, LastWill, LiveOnlyFlag,
        ProtocolMajorVersion, ProtocolVersion, ProtocolVersionSegment, RequestPattern,
        SendTracesFlag, TransactionId, UniqueFlag, Value,
    },
    socket::TcpSocketConfig,
};

#[derive(Debug, Clone, PartialEq)]
struct SubscriptionInfo {
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    live_only: bool,
    aggregate_duration: Duration,
    channel_buffer_size: usize,
}

pub type InsertedValues = Vec<(String, (ValueEntry, bool))>;

pub struct UpdatedLocks {
    /// locks that were previously held and have now been granted again
    pub held: HashMap<ClientId, Vec<(TransactionId, Key)>>,
    /// locks that were previously held but have not been granted again because another client got them first
    pub lost: HashMap<ClientId, Vec<(TransactionId, Key)>>,
    /// locks that were previously pending. Some of these may now have already been granted, but the result still needs to be polled
    pub pending:
        HashMap<ClientId, Vec<(TransactionId, Key, LockAcquiredReceiver, LockLostReceiver)>>,
}

pub enum WbFunction {
    Get(Key, oneshot::Sender<WorterbuchResult<Value>>, Span),
    CGet(Key, oneshot::Sender<WorterbuchResult<(Value, CasVersion)>>),
    Set(
        TransactionId,
        Interface,
        Key,
        Value,
        ClientId,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    CSet(
        TransactionId,
        Interface,
        Key,
        Value,
        CasVersion,
        ClientId,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    SPubInit(
        TransactionId,
        Interface,
        Key,
        ClientId,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    SPub(
        TransactionId,
        Interface,
        Value,
        ClientId,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    Publish(
        TransactionId,
        Interface,
        Key,
        Value,
        ClientId,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    Ls(
        Option<Key>,
        oneshot::Sender<WorterbuchResult<Vec<RegularKeySegment>>>,
    ),
    PLs(
        Option<RequestPattern>,
        oneshot::Sender<WorterbuchResult<Vec<RegularKeySegment>>>,
    ),
    PGet(
        RequestPattern,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Subscribe(
        ClientId,
        TransactionId,
        Interface,
        Key,
        UniqueFlag,
        LiveOnlyFlag,
        SendTracesFlag,
        oneshot::Sender<WorterbuchResult<Subscription>>,
    ),
    PSubscribe(
        ClientId,
        TransactionId,
        Interface,
        RequestPattern,
        UniqueFlag,
        LiveOnlyFlag,
        SendTracesFlag,
        oneshot::Sender<WorterbuchResult<PSubscription>>,
    ),
    SubscribeLs(
        ClientId,
        TransactionId,
        Interface,
        Option<Key>,
        SendTracesFlag,
        oneshot::Sender<WorterbuchResult<LsSubscription>>,
    ),
    Unsubscribe(
        ClientId,
        TransactionId,
        Interface,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    UnsubscribeLs(
        ClientId,
        TransactionId,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    Delete(
        TransactionId,
        Interface,
        Key,
        ClientId,
        oneshot::Sender<WorterbuchResult<Value>>,
    ),
    PDelete(
        TransactionId,
        Interface,
        RequestPattern,
        Option<bool>,
        ClientId,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Lock(
        TransactionId,
        Interface,
        Key,
        ClientId,
        oneshot::Sender<WorterbuchResult<LockLostReceiver>>,
    ),
    AcquireLock(
        TransactionId,
        Interface,
        Key,
        ClientId,
        oneshot::Sender<WorterbuchResult<(LockAcquiredReceiver, LockLostReceiver)>>,
    ),
    ReleaseLock(
        TransactionId,
        Interface,
        Key,
        ClientId,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    Connected(
        ClientId,
        Option<SocketAddr>,
        Protocol,
        mpsc::Sender<()>,                      // signal for ejecting this client
        oneshot::Sender<WorterbuchResult<()>>, // response channel
    ),
    ProtocolSwitched(ClientId, Interface, ProtocolMajorVersion),
    Disconnected(ClientId, Protocol, Option<SocketAddr>),
    Config(oneshot::Sender<Config>),
    Export(
        oneshot::Sender<(
            Value,
            HashMap<ClientId, GraveGoods>,
            HashMap<ClientId, LastWill>,
        )>,
        Span,
    ),
    Import(
        TransactionId,
        ClientId,
        Interface,
        String,
        oneshot::Sender<WorterbuchResult<InsertedValues>>,
    ),
    Len(oneshot::Sender<usize>),
    ReGrantLocks(Locks, oneshot::Sender<WorterbuchResult<UpdatedLocks>>),
}

impl fmt::Debug for WbFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WbFunction::Get(key, _, _) => f.debug_tuple("Get").field(key).finish(),
            WbFunction::CGet(key, _) => f.debug_tuple("CGet").field(key).finish(),
            WbFunction::Set(transaction_id, interface, key, value, client_id, _, _) => f
                .debug_tuple("Set")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(value)
                .field(client_id)
                .finish(),
            WbFunction::CSet(transaction_id, interface, key, value, version, client_id, _) => f
                .debug_tuple("CSet")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(value)
                .field(version)
                .field(client_id)
                .finish(),
            WbFunction::SPubInit(transaction_id, interface, key, client_id, _) => f
                .debug_tuple("SPubInit")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(client_id)
                .finish(),
            WbFunction::SPub(transaction_id, interface, value, client_id, _) => f
                .debug_tuple("SPub")
                .field(transaction_id)
                .field(interface)
                .field(value)
                .field(client_id)
                .finish(),
            WbFunction::Publish(transaction_id, interface, key, value, client_id, _) => f
                .debug_tuple("Publish")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(value)
                .field(client_id)
                .finish(),
            WbFunction::Ls(parent, _) => f.debug_tuple("Ls").field(parent).finish(),
            WbFunction::PLs(parent, _) => f.debug_tuple("PLs").field(parent).finish(),
            WbFunction::PGet(pattern, _) => f.debug_tuple("PGet").field(pattern).finish(),
            WbFunction::Subscribe(
                client_id,
                transaction_id,
                interface,
                key,
                unique,
                live_only,
                send_traces,
                _,
            ) => f
                .debug_tuple("Subscribe")
                .field(client_id)
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(unique)
                .field(live_only)
                .field(send_traces)
                .finish(),
            WbFunction::PSubscribe(
                client_id,
                transaction_id,
                interface,
                pattern,
                unique,
                live_only,
                send_traces,
                _,
            ) => f
                .debug_tuple("PSubscribe")
                .field(client_id)
                .field(transaction_id)
                .field(interface)
                .field(pattern)
                .field(unique)
                .field(live_only)
                .field(send_traces)
                .finish(),
            WbFunction::SubscribeLs(
                client_id,
                transaction_id,
                interface,
                parent,
                send_traces,
                _,
            ) => f
                .debug_tuple("SubscribeLs")
                .field(client_id)
                .field(transaction_id)
                .field(interface)
                .field(parent)
                .field(send_traces)
                .finish(),
            WbFunction::Unsubscribe(client_id, transaction_id, interface, _) => f
                .debug_tuple("Unsubscribe")
                .field(client_id)
                .field(transaction_id)
                .field(interface)
                .finish(),
            WbFunction::UnsubscribeLs(client_id, transaction_id, _) => f
                .debug_tuple("UnsubscribeLs")
                .field(client_id)
                .field(transaction_id)
                .finish(),
            WbFunction::Delete(transaction_id, interface, key, client_id, _) => f
                .debug_tuple("Delete")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(client_id)
                .finish(),
            WbFunction::PDelete(transaction_id, interface, pattern, quiet, client_id, _) => f
                .debug_tuple("PDelete")
                .field(transaction_id)
                .field(interface)
                .field(pattern)
                .field(quiet)
                .field(client_id)
                .finish(),
            WbFunction::Lock(transaction_id, interface, key, client_id, _) => f
                .debug_tuple("Lock")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(client_id)
                .finish(),
            WbFunction::AcquireLock(transaction_id, interface, key, client_id, _) => f
                .debug_tuple("AcquireLock")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(client_id)
                .finish(),
            WbFunction::ReleaseLock(transaction_id, interface, key, client_id, _) => f
                .debug_tuple("ReleaseLock")
                .field(transaction_id)
                .field(interface)
                .field(key)
                .field(client_id)
                .finish(),
            WbFunction::Connected(client_id, remote_addr, protocol, _, _) => f
                .debug_tuple("Connected")
                .field(client_id)
                .field(remote_addr)
                .field(protocol)
                .finish(),
            WbFunction::ProtocolSwitched(client_id, interface, version) => f
                .debug_tuple("ProtocolSwitched")
                .field(client_id)
                .field(interface)
                .field(version)
                .finish(),
            WbFunction::Disconnected(client_id, protocol, remote_addr) => f
                .debug_tuple("Disconnected")
                .field(client_id)
                .field(protocol)
                .field(remote_addr)
                .finish(),
            WbFunction::Config(_) => f.debug_tuple("Config").finish(),
            WbFunction::Export(_, _) => f.debug_tuple("Export").finish(),
            WbFunction::Import(transaction_id, client_id, interface, path, _) => f
                .debug_tuple("Import")
                .field(transaction_id)
                .field(client_id)
                .field(interface)
                .field(path)
                .finish(),
            WbFunction::ReGrantLocks(locks, _) => {
                f.debug_tuple("ReGrantLocks").field(locks).finish()
            }
            WbFunction::Len(_) => f.debug_tuple("Len").finish(),
        }
    }
}

#[derive(Clone)]
pub struct CloneableWbApi {
    name: String,
    config: Config,
    tx: mpsc::Sender<WbFunction>,
    interface: Interface,
    supported_client_protocol_versions: Box<[ProtocolVersionSegment]>,
}

impl Drop for CloneableWbApi {
    fn drop(&mut self) {
        debug!("Dropping CloneableWbApi '{}'", self);
        if self.tx.is_closed() {
            debug!("CloneableWbApi '{}' tx channel is already closed", self);
        }
    }
}

impl CloneableWbApi {
    pub fn new(
        tx: mpsc::Sender<WbFunction>,
        config: Config,
        interface: Interface,
        supported_client_protocol_versions: &[ProtocolVersion],
    ) -> Self {
        CloneableWbApi {
            name: "".to_string(),
            tx,
            config,
            interface,
            supported_client_protocol_versions: supported_client_protocol_versions
                .iter()
                .map(ProtocolVersion::major)
                .collect(),
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn named(&self, name: impl fmt::Display) -> Self {
        CloneableWbApi {
            name: format!("{}/{}", self.name, name),
            config: self.config.clone(),
            tx: self.tx.clone(),
            interface: self.interface.clone(),
            supported_client_protocol_versions: self.supported_client_protocol_versions.clone(),
        }
    }

    pub fn for_interface(&self, name: impl fmt::Display, interface: Interface) -> Self {
        CloneableWbApi {
            name: format!("{}/{}", self.name, name),
            config: self.config.clone(),
            tx: self.tx.clone(),
            interface,
            supported_client_protocol_versions: self.supported_client_protocol_versions.clone(),
        }
    }

    pub(crate) async fn re_grant_locks(&self, locks: Locks) -> WorterbuchResult<UpdatedLocks> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::ReGrantLocks(locks, tx);
        self.request_with_timeout(function, "re grant locks", rx)
            .await
    }

    async fn request_with_timeout<T>(
        &self,
        wb_function: WbFunction,
        request: &str,
        rx: oneshot::Receiver<WorterbuchResult<T>>,
    ) -> WorterbuchResult<T> {
        self.send_with_timeout(wb_function, request).await?;
        trace!(self.name, request, "waiting for response to request …");
        let res = rx.await;
        trace!(self.name, request, "received response");
        res?
    }

    async fn send_with_timeout(
        &self,
        wb_function: WbFunction,
        request: &str,
    ) -> WorterbuchResult<()> {
        trace!(self.name, request, "sending request to core system …",);
        if timeout(
            self.config.channel_buffer_timeout,
            self.tx.send(wb_function),
        )
        .await
        .is_err()
        {
            return Err(WorterbuchError::InternalChannelTimeout(format!(
                "timeout while sending '{}' request to core system",
                request
            )));
        }
        trace!(self.name, request, "sent request to core system");
        Ok(())
    }
}

impl fmt::Display for CloneableWbApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = if self.name.is_empty() {
            "<root>"
        } else {
            &self.name
        };
        name.fmt(f)
    }
}

impl WbApi for CloneableWbApi {
    fn supported_protocol_versions(&self) -> Box<[ProtocolVersion]> {
        self.config.supported_client_protocol_versions()
    }

    fn version(&self) -> &str {
        VERSION
    }

    #[instrument(level = Level::TRACE, skip_all, err)]
    async fn get(&self, key: Key) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Get(key, tx, Span::current());
        self.request_with_timeout(function, "get", rx).await
    }

    async fn cget(&self, key: Key) -> WorterbuchResult<(Value, CasVersion)> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::CGet(key, tx);
        self.request_with_timeout(function, "cget", rx).await
    }

    async fn pget(&self, pattern: RequestPattern) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PGet(pattern, tx);
        self.request_with_timeout(function, "pget", rx).await
    }

    #[instrument(level=Level::TRACE, skip(self), err)]
    async fn set(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Set(
            transaction_id,
            self.interface.clone(),
            key,
            value,
            client_id,
            tx,
            Span::current(),
        );
        self.request_with_timeout(function, "set", rx).await
    }

    async fn cset(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        version: CasVersion,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::CSet(
            transaction_id,
            self.interface.clone(),
            key,
            value,
            version,
            client_id,
            tx,
        );
        self.request_with_timeout(function, "cset", rx).await
    }

    async fn lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<LockLostReceiver> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Lock(transaction_id, self.interface.clone(), key, client_id, tx);
        self.request_with_timeout(function, "lock", rx).await
    }

    async fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<(LockAcquiredReceiver, LockLostReceiver)> {
        let (tx, rx) = oneshot::channel();
        let function =
            WbFunction::AcquireLock(transaction_id, self.interface.clone(), key, client_id, tx);
        self.request_with_timeout(function, "acquire lock", rx)
            .await
    }

    async fn release_lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function =
            WbFunction::ReleaseLock(transaction_id, self.interface.clone(), key, client_id, tx);
        self.request_with_timeout(function, "release lock", rx)
            .await
    }

    async fn spub_init(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function =
            WbFunction::SPubInit(transaction_id, self.interface.clone(), key, client_id, tx);
        self.request_with_timeout(function, "spub init", rx).await
    }

    async fn spub(
        &self,
        transaction_id: TransactionId,
        value: Value,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function =
            WbFunction::SPub(transaction_id, self.interface.clone(), value, client_id, tx);
        self.request_with_timeout(function, "spub", rx).await
    }

    async fn publish(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Publish(
            transaction_id,
            self.interface.clone(),
            key,
            value,
            client_id,
            tx,
        );
        self.request_with_timeout(function, "publish", rx).await
    }

    async fn ls(&self, parent: Option<Key>) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Ls(parent, tx);
        self.request_with_timeout(function, "ls", rx).await
    }

    async fn pls(
        &self,
        parent: Option<RequestPattern>,
    ) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PLs(parent, tx);
        self.request_with_timeout(function, "pls", rx).await
    }

    async fn subscribe(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        key: Key,
        unique: UniqueFlag,
        live_only: LiveOnlyFlag,
        send_traces: SendTracesFlag,
    ) -> WorterbuchResult<Subscription> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Subscribe(
            client_id,
            transaction_id,
            self.interface.clone(),
            key,
            unique,
            live_only,
            send_traces,
            tx,
        );
        self.request_with_timeout(function, "subscribe", rx).await
    }

    async fn psubscribe(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: UniqueFlag,
        live_only: LiveOnlyFlag,
        send_traces: SendTracesFlag,
    ) -> WorterbuchResult<PSubscription> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PSubscribe(
            client_id,
            transaction_id,
            self.interface.clone(),
            pattern,
            unique,
            live_only,
            send_traces,
            tx,
        );
        self.request_with_timeout(function, "psubscribe", rx).await
    }

    async fn subscribe_ls(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        parent: Option<Key>,
        send_traces: SendTracesFlag,
    ) -> WorterbuchResult<LsSubscription> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::SubscribeLs(
            client_id,
            transaction_id,
            self.interface.clone(),
            parent,
            send_traces,
            tx,
        );
        self.request_with_timeout(function, "subscribe ls", rx)
            .await
    }

    async fn unsubscribe(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function =
            WbFunction::Unsubscribe(client_id, transaction_id, self.interface.clone(), tx);
        self.request_with_timeout(function, "unsubscribe", rx).await
    }

    async fn unsubscribe_ls(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::UnsubscribeLs(client_id, transaction_id, tx);
        self.request_with_timeout(function, "unsubscribe ls", rx)
            .await
    }

    async fn delete(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        let function =
            WbFunction::Delete(transaction_id, self.interface.clone(), key, client_id, tx);
        self.request_with_timeout(function, "delete", rx).await
    }

    async fn pdelete(
        &self,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        quiet: Option<bool>,
        client_id: ClientId,
    ) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PDelete(
            transaction_id,
            self.interface.clone(),
            pattern,
            quiet,
            client_id,
            tx,
        );
        self.request_with_timeout(function, "pdelete", rx).await
    }

    #[instrument(level=Level::TRACE, skip(self))]
    async fn connected(
        &self,
        client_id: ClientId,
        remote_addr: Option<SocketAddr>,
        protocol: Protocol,
    ) -> WorterbuchResult<mpsc::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let (eject, eject_rx) = mpsc::channel(1);
        let function = WbFunction::Connected(client_id, remote_addr, protocol, eject, tx);
        self.request_with_timeout(function, "connected", rx).await?;
        Ok(eject_rx)
    }

    #[instrument(level=Level::TRACE, skip(self))]
    async fn protocol_switched(
        &self,
        client_id: ClientId,
        protocol: ProtocolMajorVersion,
    ) -> WorterbuchResult<()> {
        let function = WbFunction::ProtocolSwitched(client_id, self.interface.clone(), protocol);
        self.send_with_timeout(function, "protocol switched")
            .await?;
        Ok(())
    }

    #[instrument(level=Level::TRACE, skip(self))]
    async fn disconnected(
        &self,
        client_id: ClientId,
        protocol: Protocol,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        let function = WbFunction::Disconnected(client_id, protocol, remote_addr);
        self.send_with_timeout(function, "disconnected").await?;
        Ok(())
    }

    async fn export(
        &self,
        span: Span,
    ) -> WorterbuchResult<(
        Value,
        HashMap<ClientId, GraveGoods>,
        HashMap<ClientId, LastWill>,
    )> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Export(tx, span);
        self.send_with_timeout(function, "export").await?;
        Ok(rx.await?)
    }

    async fn import(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        json: String,
    ) -> WorterbuchResult<Vec<(String, (ValueEntry, bool))>> {
        let (tx, rx) = oneshot::channel();
        let function =
            WbFunction::Import(transaction_id, client_id, self.interface.clone(), json, tx);
        self.request_with_timeout(function, "import", rx).await
    }

    async fn entries(&self) -> WorterbuchResult<usize> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Len(tx);
        self.send_with_timeout(function, "entries").await?;
        Ok(rx.await?)
    }
}

impl From<&Config> for TcpSocketConfig {
    fn from(config: &Config) -> Self {
        Self {
            keepalive_time: config.keepalive_time,
            keepalive_interval: config.keepalive_interval,
            keepalive_retries: config.keepalive_retries,
            send_timeout: config.send_timeout,
        }
    }
}
