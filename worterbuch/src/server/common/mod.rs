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

use crate::{Config, INTERNAL_CLIENT_ID, server::CloneableWbApi, stats::VERSION};
use miette::{IntoDiagnostic, Result};
use socket2::{Domain, Protocol as SockProto, SockAddr, Socket, TcpKeepalive, Type};
use std::{
    net::{IpAddr, SocketAddr, TcpListener},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{Level, Span, instrument, trace};
use uuid::Uuid;
use worterbuch_common::{
    CasVersion, GraveGoods, Key, KeyValuePairs, LastWill, LiveOnlyFlag, PStateEvent, Protocol,
    ProtocolMajorVersion, ProtocolVersion, RegularKeySegment, RequestPattern, StateEvent,
    SubscriptionId, TransactionId, UniqueFlag, Value, ValueEntry, WbApi, error::WorterbuchResult,
};

pub const SUPPORTED_PROTOCOL_VERSIONS: [ProtocolVersion; 2] =
    [ProtocolVersion::new(0, 11), ProtocolVersion::new(1, 1)];

#[derive(Debug, Clone, PartialEq)]
struct SubscriptionInfo {
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    live_only: bool,
    aggregate_duration: Duration,
    channel_buffer_size: usize,
}

pub type InsertedValues = Vec<(String, (ValueEntry, bool))>;

pub enum WbFunction {
    Get(Key, oneshot::Sender<WorterbuchResult<Value>>),
    CGet(Key, oneshot::Sender<WorterbuchResult<(Value, CasVersion)>>),
    Set(
        Key,
        Value,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
        Span,
    ),
    CSet(
        Key,
        Value,
        CasVersion,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    SPubInit(
        TransactionId,
        Key,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    SPub(
        TransactionId,
        Value,
        Uuid,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    Publish(Key, Value, oneshot::Sender<WorterbuchResult<()>>),
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
        Uuid,
        TransactionId,
        Key,
        UniqueFlag,
        LiveOnlyFlag,
        oneshot::Sender<WorterbuchResult<(mpsc::Receiver<StateEvent>, SubscriptionId)>>,
    ),
    PSubscribe(
        Uuid,
        TransactionId,
        RequestPattern,
        UniqueFlag,
        LiveOnlyFlag,
        oneshot::Sender<WorterbuchResult<(mpsc::Receiver<PStateEvent>, SubscriptionId)>>,
    ),
    SubscribeLs(
        Uuid,
        TransactionId,
        Option<Key>,
        oneshot::Sender<WorterbuchResult<(mpsc::Receiver<Vec<RegularKeySegment>>, SubscriptionId)>>,
    ),
    Unsubscribe(Uuid, TransactionId, oneshot::Sender<WorterbuchResult<()>>),
    UnsubscribeLs(Uuid, TransactionId, oneshot::Sender<WorterbuchResult<()>>),
    Delete(Key, Uuid, oneshot::Sender<WorterbuchResult<Value>>),
    PDelete(
        RequestPattern,
        Uuid,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Lock(Key, Uuid, oneshot::Sender<WorterbuchResult<()>>),
    AcquireLock(
        Key,
        Uuid,
        oneshot::Sender<WorterbuchResult<oneshot::Receiver<()>>>,
    ),
    ReleaseLock(Key, Uuid, oneshot::Sender<WorterbuchResult<()>>),
    Connected(Uuid, Option<SocketAddr>, Protocol),
    ProtocolSwitched(Uuid, ProtocolMajorVersion),
    Disconnected(Uuid, Option<SocketAddr>),
    Config(oneshot::Sender<Config>),
    Export(oneshot::Sender<(Value, GraveGoods, LastWill)>, Span),
    Import(String, oneshot::Sender<WorterbuchResult<InsertedValues>>),
    Len(oneshot::Sender<usize>),
}

impl CloneableWbApi {
    pub fn new(tx: mpsc::Sender<WbFunction>, config: Config) -> Self {
        CloneableWbApi { tx, config }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl WbApi for CloneableWbApi {
    fn supported_protocol_versions(&self) -> Vec<ProtocolVersion> {
        SUPPORTED_PROTOCOL_VERSIONS.into()
    }

    fn version(&self) -> &str {
        VERSION
    }

    async fn get(&self, key: Key) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Get(key, tx)).await?;
        rx.await?
    }

    async fn cget(&self, key: Key) -> WorterbuchResult<(Value, CasVersion)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::CGet(key, tx)).await?;
        rx.await?
    }

    async fn pget(&self, pattern: RequestPattern) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::PGet(pattern, tx)).await?;
        rx.await?
    }

    #[instrument(level=Level::TRACE, skip(self))]
    async fn set(&self, key: Key, value: Value, client_id: Uuid) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();

        trace!("Sending set request to core system …");
        let res = self
            .tx
            .send(WbFunction::Set(key, value, client_id, tx, Span::current()))
            .await;
        trace!("Sending set request to core system done.");
        res?;
        trace!("Waiting for response to set request …");
        let res = rx.await;
        trace!("Waiting for response to set request done.");
        res?
    }

    async fn cset(
        &self,
        key: Key,
        value: Value,
        version: CasVersion,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending set request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::CSet(key, value, version, client_id, tx))
            .await;
        if trace {
            trace!("Sending set request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to cset request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to cset request done.");
        }
        res?
    }

    async fn lock(&self, key: Key, client_id: Uuid) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending lock request to core system …");
        }
        let res = self.tx.send(WbFunction::Lock(key, client_id, tx)).await;
        if trace {
            trace!("Sending lock request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to lock request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to lock request done.");
        }
        res?
    }

    async fn acquire_lock(
        &self,
        key: Key,
        client_id: Uuid,
    ) -> WorterbuchResult<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending lock request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::AcquireLock(key, client_id, tx))
            .await;
        if trace {
            trace!("Sending lock request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to lock request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to lock request done.");
        }
        res?
    }

    async fn release_lock(&self, key: Key, client_id: Uuid) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending release lock request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::ReleaseLock(key, client_id, tx))
            .await;
        if trace {
            trace!("Sending release lock request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to release lock request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to release lock request done.");
        }
        res?
    }

    async fn spub_init(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending set request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::SPubInit(transaction_id, key, client_id, tx))
            .await;
        if trace {
            trace!("Sending set request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to set request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to set request done.");
        }
        res?
    }

    async fn spub(
        &self,
        transaction_id: TransactionId,
        value: Value,
        client_id: Uuid,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending set request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::SPub(transaction_id, value, client_id, tx))
            .await;
        if trace {
            trace!("Sending set request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to set request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to set request done.");
        }
        res?
    }

    async fn publish(&self, key: Key, value: Value) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Publish(key, value, tx)).await?;
        rx.await?
    }

    async fn ls(&self, parent: Option<Key>) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Ls(parent, tx)).await?;
        rx.await?
    }

    async fn pls(
        &self,
        parent: Option<RequestPattern>,
    ) -> WorterbuchResult<Vec<RegularKeySegment>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::PLs(parent, tx)).await?;
        rx.await?
    }

    async fn subscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        key: Key,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(mpsc::Receiver<StateEvent>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Subscribe(
                client_id,
                transaction_id,
                key,
                unique,
                live_only,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn psubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: bool,
        live_only: bool,
    ) -> WorterbuchResult<(mpsc::Receiver<PStateEvent>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::PSubscribe(
                client_id,
                transaction_id,
                pattern,
                unique,
                live_only,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn subscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
        parent: Option<Key>,
    ) -> WorterbuchResult<(mpsc::Receiver<Vec<RegularKeySegment>>, SubscriptionId)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::SubscribeLs(
                client_id,
                transaction_id,
                parent,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn unsubscribe(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Unsubscribe(client_id, transaction_id, tx))
            .await?;
        rx.await?
    }

    async fn unsubscribe_ls(
        &self,
        client_id: Uuid,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::UnsubscribeLs(client_id, transaction_id, tx))
            .await?;
        rx.await?
    }

    async fn delete(&self, key: Key, client_id: Uuid) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Delete(key, client_id, tx)).await?;
        rx.await?
    }

    async fn pdelete(
        &self,
        pattern: RequestPattern,
        client_id: Uuid,
    ) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::PDelete(pattern, client_id, tx))
            .await?;
        rx.await?
    }

    async fn connected(
        &self,
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
        protocol: Protocol,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::Connected(client_id, remote_addr, protocol))
            .await?;
        Ok(())
    }

    async fn protocol_switched(
        &self,
        client_id: Uuid,
        protocol: ProtocolMajorVersion,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::ProtocolSwitched(client_id, protocol))
            .await?;
        Ok(())
    }

    async fn disconnected(
        &self,
        client_id: Uuid,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::Disconnected(client_id, remote_addr))
            .await?;
        Ok(())
    }

    async fn export(&self, span: Span) -> WorterbuchResult<(Value, GraveGoods, LastWill)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Export(tx, span)).await?;
        Ok(rx.await?)
    }

    async fn import(&self, json: String) -> WorterbuchResult<Vec<(String, (ValueEntry, bool))>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Import(json, tx)).await?;
        rx.await?
    }

    async fn len(&self) -> WorterbuchResult<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Len(tx)).await?;
        Ok(rx.await?)
    }
}

pub fn init_server_socket(bind_addr: IpAddr, port: u16, config: Config) -> Result<TcpListener> {
    let addr = format!("{bind_addr}:{port}");
    let addr: SocketAddr = addr.parse().into_diagnostic()?;

    let mut tcp_keepalive = TcpKeepalive::new();
    if let Some(keepalive) = config.keepalive_time {
        tcp_keepalive = tcp_keepalive.with_time(keepalive);
    }
    if let Some(keepalive) = config.keepalive_interval {
        tcp_keepalive = tcp_keepalive.with_interval(keepalive);
    }
    if let Some(retries) = config.keepalive_retries {
        tcp_keepalive = tcp_keepalive.with_retries(retries);
    }

    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(SockProto::TCP)).into_diagnostic()?;

    #[cfg(not(target_os = "windows"))]
    socket.set_reuse_address(true).into_diagnostic()?;
    socket.set_nonblocking(true).into_diagnostic()?;
    socket.set_keepalive(true).into_diagnostic()?;
    socket.set_tcp_keepalive(&tcp_keepalive).into_diagnostic()?;
    #[cfg(target_os = "linux")]
    socket
        .set_tcp_user_timeout(config.send_timeout)
        .into_diagnostic()?;
    socket.set_tcp_nodelay(true).into_diagnostic()?;
    socket.bind(&SockAddr::from(addr)).into_diagnostic()?;
    socket.listen(1024).into_diagnostic()?;
    let listener = socket.into();

    Ok(listener)
}
