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
use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tracing::{Level, Span, debug, instrument, trace};
use worterbuch_common::{
    ClientId, Export, LockAcquiredReceiver, LockLostReceiver, LsSubscription, PSubscription,
    Protocol, RegularKeySegment, Subscription, ValueEntry, WbApi,
    error::{WorterbuchError, WorterbuchResult},
    protocol::v1::{
        CSet, CasVersion, ClientInfo, Delete, Get, GraveGoods, Key, KeyValuePairs, LastWill, Lock,
        Ls, PDelete, PGet, PLs, PSubscribe, ProtocolMajorVersion, ProtocolVersion,
        ProtocolVersionSegment, Publish, RequestPattern, SPub, SPubInit, Set, Subscribe,
        SubscribeLs, TransactionId, Unsubscribe, UnsubscribeLs, Value,
    },
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
    Get(
        Get,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<Value>>,
        Span,
    ),
    CGet(
        Get,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<(Value, CasVersion)>>,
        Span,
    ),
    Set(Set, ClientInfo, oneshot::Sender<WorterbuchResult<()>>, Span),
    CSet(
        CSet,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    SPubInit(
        SPubInit,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    SPub(
        SPub,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    Publish(
        Publish,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    Ls(
        Ls,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<Vec<RegularKeySegment>>>,
        Span,
    ),
    PLs(
        PLs,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<Vec<RegularKeySegment>>>,
        Span,
    ),
    PGet(
        PGet,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
        Span,
    ),
    Subscribe(
        Subscribe,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<Subscription>>,
        Span,
    ),
    PSubscribe(
        PSubscribe,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<PSubscription>>,
        Span,
    ),
    SubscribeLs(
        SubscribeLs,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<LsSubscription>>,
        Span,
    ),
    Unsubscribe(
        Unsubscribe,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    UnsubscribeLs(
        UnsubscribeLs,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    Delete(
        Delete,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<Value>>,
        Span,
    ),
    PDelete(
        PDelete,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
        Span,
    ),
    Lock(
        Lock,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<LockLostReceiver>>,
        Span,
    ),
    AcquireLock(
        Lock,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<(LockAcquiredReceiver, LockLostReceiver)>>,
        Span,
    ),
    ReleaseLock(
        Lock,
        ClientInfo,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    Connected(
        ClientInfo,
        Option<SocketAddr>,
        mpsc::Sender<()>,                      // signal for ejecting this client
        oneshot::Sender<WorterbuchResult<()>>, // response channel
        Span,
    ),
    ProtocolSwitched(ClientInfo, ProtocolMajorVersion, Span),
    Disconnected(ClientInfo, Option<SocketAddr>, Span),
    Config(oneshot::Sender<Config>, Span),
    Export(oneshot::Sender<Export>, Span),
    Import(
        TransactionId,
        ClientInfo,
        String,
        oneshot::Sender<WorterbuchResult<InsertedValues>>,
        Span,
    ),
    Len(oneshot::Sender<usize>, Span),
    Tick(Span),
    ReGrantLocks(Locks, oneshot::Sender<WorterbuchResult<UpdatedLocks>>, Span),
}

impl fmt::Debug for WbFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WbFunction::Get(req, ..) => req.fmt(f),
            WbFunction::CGet(req, ..) => req.fmt(f),
            WbFunction::Set(req, ..) => req.fmt(f),
            WbFunction::CSet(req, ..) => req.fmt(f),
            WbFunction::SPubInit(req, ..) => req.fmt(f),
            WbFunction::SPub(req, ..) => req.fmt(f),
            WbFunction::Publish(req, ..) => req.fmt(f),
            WbFunction::Ls(req, ..) => req.fmt(f),
            WbFunction::PLs(req, ..) => req.fmt(f),
            WbFunction::PGet(req, ..) => req.fmt(f),
            WbFunction::Subscribe(req, ..) => req.fmt(f),
            WbFunction::PSubscribe(req, ..) => req.fmt(f),
            WbFunction::SubscribeLs(req, ..) => req.fmt(f),
            WbFunction::Unsubscribe(req, ..) => req.fmt(f),
            WbFunction::UnsubscribeLs(req, ..) => req.fmt(f),
            WbFunction::Delete(req, ..) => req.fmt(f),
            WbFunction::PDelete(req, ..) => req.fmt(f),
            WbFunction::Lock(req, ..) => req.fmt(f),
            WbFunction::AcquireLock(req, ..) => req.fmt(f),
            WbFunction::ReleaseLock(req, ..) => req.fmt(f),
            WbFunction::Connected(client_info, remote_addr, ..) => f
                .debug_tuple("Connected")
                .field(client_info)
                .field(remote_addr)
                .finish(),
            WbFunction::ProtocolSwitched(client_info, version, _) => f
                .debug_tuple("ProtocolSwitched")
                .field(client_info)
                .field(version)
                .finish(),
            WbFunction::Disconnected(client_info, remote_addr, _) => f
                .debug_tuple("Disconnected")
                .field(client_info)
                .field(remote_addr)
                .finish(),
            WbFunction::Config(_, _) => f.debug_tuple("Config").finish(),
            WbFunction::Export(_, _) => f.debug_tuple("Export").finish(),
            WbFunction::Import(transaction_id, client_info, path, _, _) => f
                .debug_tuple("Import")
                .field(transaction_id)
                .field(client_info)
                .field(path)
                .finish(),
            WbFunction::ReGrantLocks(locks, _, _) => {
                f.debug_tuple("ReGrantLocks").field(locks).finish()
            }
            WbFunction::Len(_, _) => f.debug_tuple("Len").finish(),
            WbFunction::Tick(_) => f.debug_tuple("Tick").finish(),
        }
    }
}

/// Cheap to clone: a handle like this is held by every client connection and every
/// subscription, so the config and protocol versions are shared instead of copied.
#[derive(Clone)]
pub struct CloneableWbApi {
    name: String,
    config: Arc<Config>,
    tx: mpsc::Sender<WbFunction>,
    protocol: Protocol,
    supported_client_protocol_versions: Arc<[ProtocolVersionSegment]>,
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
        protocol: Protocol,
        supported_client_protocol_versions: &[ProtocolVersion],
    ) -> Self {
        CloneableWbApi {
            name: "".to_string(),
            tx,
            config: Arc::new(config),
            protocol,
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
            protocol: self.protocol.clone(),
            supported_client_protocol_versions: self.supported_client_protocol_versions.clone(),
        }
    }

    pub fn for_protocol(&self, name: impl fmt::Display, protocol: Protocol) -> Self {
        CloneableWbApi {
            name: format!("{}/{}", self.name, name),
            config: self.config.clone(),
            tx: self.tx.clone(),
            protocol,
            supported_client_protocol_versions: self.supported_client_protocol_versions.clone(),
        }
    }

    pub(crate) async fn re_grant_locks(&self, locks: Locks) -> WorterbuchResult<UpdatedLocks> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::ReGrantLocks(locks, tx, Span::current());
        self.request_with_timeout(function, "re grant locks", rx)
            .await
    }

    /// Triggers periodic maintenance in the core system, see [`crate::worterbuch::Worterbuch::tick`].
    pub(crate) async fn tick(&self) -> WorterbuchResult<()> {
        self.send_with_timeout(WbFunction::Tick(Span::current()), "tick")
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
        if self
            .tx
            .send_timeout(wb_function, self.config.channel_buffer_timeout)
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

impl CloneableWbApi {
    #[instrument(level=Level::TRACE, skip(self))]
    pub async fn connected(
        &self,
        client_info: ClientInfo,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<mpsc::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let (eject, eject_rx) = mpsc::channel(1);
        let function = WbFunction::Connected(client_info, remote_addr, eject, tx, Span::current());
        self.request_with_timeout(function, "connected", rx).await?;
        Ok(eject_rx)
    }

    #[instrument(level=Level::TRACE, skip(self))]
    pub async fn protocol_switched(
        &self,
        client_info: ClientInfo,
        protocol: ProtocolMajorVersion,
    ) -> WorterbuchResult<()> {
        let function = WbFunction::ProtocolSwitched(client_info, protocol, Span::current());
        self.send_with_timeout(function, "protocol switched")
            .await?;
        Ok(())
    }

    #[instrument(level=Level::TRACE, skip(self))]
    pub async fn disconnected(
        &self,
        client_info: ClientInfo,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        let function = WbFunction::Disconnected(client_info, remote_addr, Span::current());
        self.send_with_timeout(function, "disconnected").await?;
        Ok(())
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
    async fn get(&self, req: Get, client_info: ClientInfo) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Get(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "get", rx).await
    }

    async fn cget(
        &self,
        req: Get,
        client_info: ClientInfo,
    ) -> WorterbuchResult<(Value, CasVersion)> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::CGet(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "cget", rx).await
    }

    async fn pget(&self, req: PGet, client_info: ClientInfo) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PGet(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "pget", rx).await
    }

    #[instrument(level=Level::TRACE, skip(self), err)]
    async fn set(&self, req: Set, client_info: ClientInfo) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Set(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "set", rx).await
    }

    async fn cset(&self, req: CSet, client_info: ClientInfo) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::CSet(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "cset", rx).await
    }

    async fn lock(&self, req: Lock, client_info: ClientInfo) -> WorterbuchResult<LockLostReceiver> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Lock(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "lock", rx).await
    }

    async fn acquire_lock(
        &self,
        req: Lock,
        client_info: ClientInfo,
    ) -> WorterbuchResult<(LockAcquiredReceiver, LockLostReceiver)> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::AcquireLock(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "acquire lock", rx)
            .await
    }

    async fn release_lock(&self, req: Lock, client_info: ClientInfo) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::ReleaseLock(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "release lock", rx)
            .await
    }

    async fn spub_init(&self, req: SPubInit, client_info: ClientInfo) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::SPubInit(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "spub init", rx).await
    }

    async fn spub(&self, req: SPub, client_info: ClientInfo) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::SPub(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "spub", rx).await
    }

    async fn publish(&self, req: Publish, client_info: ClientInfo) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Publish(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "publish", rx).await
    }

    async fn ls(
        &self,
        req: Ls,
        client_info: ClientInfo,
    ) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Ls(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "ls", rx).await
    }

    async fn pls(
        &self,
        req: PLs,
        client_info: ClientInfo,
    ) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PLs(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "pls", rx).await
    }

    async fn subscribe(
        &self,
        req: Subscribe,
        client_info: ClientInfo,
    ) -> WorterbuchResult<Subscription> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Subscribe(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "subscribe", rx).await
    }

    async fn psubscribe(
        &self,
        req: PSubscribe,
        client_info: ClientInfo,
    ) -> WorterbuchResult<PSubscription> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PSubscribe(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "psubscribe", rx).await
    }

    async fn subscribe_ls(
        &self,
        req: SubscribeLs,
        client_info: ClientInfo,
    ) -> WorterbuchResult<LsSubscription> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::SubscribeLs(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "subscribe ls", rx)
            .await
    }

    async fn unsubscribe(&self, req: Unsubscribe, client_info: ClientInfo) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Unsubscribe(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "unsubscribe", rx).await
    }

    async fn unsubscribe_ls(
        &self,
        req: UnsubscribeLs,
        client_info: ClientInfo,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::UnsubscribeLs(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "unsubscribe ls", rx)
            .await
    }

    async fn delete(&self, req: Delete, client_info: ClientInfo) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Delete(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "delete", rx).await
    }

    async fn pdelete(
        &self,
        req: PDelete,
        client_info: ClientInfo,
    ) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::PDelete(req, client_info, tx, Span::current());
        self.request_with_timeout(function, "pdelete", rx).await
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
        let function = WbFunction::Import(
            transaction_id,
            ClientInfo {
                client_id,
                protocol: self.protocol.clone(),
            },
            json,
            tx,
            Span::current(),
        );
        self.request_with_timeout(function, "import", rx).await
    }

    async fn entries(&self) -> WorterbuchResult<usize> {
        let (tx, rx) = oneshot::channel();
        let function = WbFunction::Len(tx, Span::current());
        self.send_with_timeout(function, "entries").await?;
        Ok(rx.await?)
    }
}
