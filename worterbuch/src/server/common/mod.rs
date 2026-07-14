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
use worterbuch_common::{
    LsSubscription, PSubscription, Protocol, RegularKeySegment, Subscription, ValueEntry, WbApi,
    error::WorterbuchResult,
    protocol::{
        CasVersion, ClientId, GraveGoods, Interface, Key, KeyValuePairs, LastWill, LiveOnlyFlag,
        ProtocolMajorVersion, ProtocolVersion, RequestPattern, SendTracesFlag, TransactionId,
        UniqueFlag, Value,
    },
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
        ClientId,
        oneshot::Sender<WorterbuchResult<KeyValuePairs>>,
    ),
    Lock(
        TransactionId,
        Interface,
        Key,
        ClientId,
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    AcquireLock(
        TransactionId,
        Interface,
        Key,
        ClientId,
        oneshot::Sender<WorterbuchResult<oneshot::Receiver<()>>>,
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
        oneshot::Sender<WorterbuchResult<()>>,
    ),
    ProtocolSwitched(ClientId, Interface, ProtocolMajorVersion),
    Disconnected(ClientId, Protocol, Option<SocketAddr>),
    Config(oneshot::Sender<Config>),
    Export(oneshot::Sender<(Value, GraveGoods, LastWill)>, Span),
    Import(
        TransactionId,
        ClientId,
        Interface,
        String,
        oneshot::Sender<WorterbuchResult<InsertedValues>>,
    ),
    Len(oneshot::Sender<usize>),
}

impl CloneableWbApi {
    pub fn new(tx: mpsc::Sender<WbFunction>, config: Config, interface: Interface) -> Self {
        CloneableWbApi {
            tx,
            config,
            interface,
        }
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn for_interface(&self, interface: Interface) -> Self {
        CloneableWbApi {
            config: self.config.clone(),
            tx: self.tx.clone(),
            interface,
        }
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
    async fn set(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();

        trace!("Sending set request to core system …");
        let res = self
            .tx
            .send(WbFunction::Set(
                transaction_id,
                self.interface.clone(),
                key,
                value,
                client_id,
                tx,
                Span::current(),
            ))
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
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        version: CasVersion,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending cSet request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::CSet(
                transaction_id,
                self.interface.clone(),
                key,
                value,
                version,
                client_id,
                tx,
            ))
            .await;
        if trace {
            trace!("Sending cSet request to core system done.");
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

    async fn lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending lock request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::Lock(
                transaction_id,
                self.interface.clone(),
                key,
                client_id,
                tx,
            ))
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

    async fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending acquire lock request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::AcquireLock(
                transaction_id,
                self.interface.clone(),
                key,
                client_id,
                tx,
            ))
            .await;
        if trace {
            trace!("Sending acquire lock request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to acquire lock request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to acquire lock request done.");
        }
        res?
    }

    async fn release_lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending release lock request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::ReleaseLock(
                transaction_id,
                self.interface.clone(),
                key,
                client_id,
                tx,
            ))
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
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending spub init request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::SPubInit(
                transaction_id,
                self.interface.clone(),
                key,
                client_id,
                tx,
            ))
            .await;
        if trace {
            trace!("Sending spub init request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to spub init request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to spub init request done.");
        }
        res?
    }

    async fn spub(
        &self,
        transaction_id: TransactionId,
        value: Value,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        let trace = client_id != INTERNAL_CLIENT_ID;
        if trace {
            trace!("Sending spub request to core system …");
        }
        let res = self
            .tx
            .send(WbFunction::SPub(transaction_id, value, client_id, tx))
            .await;
        if trace {
            trace!("Sending spub request to core system done.");
        }
        res?;
        if trace {
            trace!("Waiting for response to spub request …");
        }
        let res = rx.await;
        if trace {
            trace!("Waiting for response to spub request done.");
        }
        res?
    }

    async fn publish(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        client_id: ClientId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Publish(
                transaction_id,
                self.interface.clone(),
                key,
                value,
                client_id,
                tx,
            ))
            .await?;
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
        client_id: ClientId,
        transaction_id: TransactionId,
        key: Key,
        unique: UniqueFlag,
        live_only: LiveOnlyFlag,
        send_traces: SendTracesFlag,
    ) -> WorterbuchResult<Subscription> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Subscribe(
                client_id,
                transaction_id,
                self.interface.clone(),
                key,
                unique,
                live_only,
                send_traces,
                tx,
            ))
            .await?;
        rx.await?
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
        self.tx
            .send(WbFunction::PSubscribe(
                client_id,
                transaction_id,
                self.interface.clone(),
                pattern,
                unique,
                live_only,
                send_traces,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn subscribe_ls(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        parent: Option<Key>,
        send_traces: SendTracesFlag,
    ) -> WorterbuchResult<LsSubscription> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::SubscribeLs(
                client_id,
                transaction_id,
                self.interface.clone(),
                parent,
                send_traces,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn unsubscribe(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Unsubscribe(
                client_id,
                transaction_id,
                self.interface.clone(),
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn unsubscribe_ls(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::UnsubscribeLs(client_id, transaction_id, tx))
            .await?;
        rx.await?
    }

    async fn delete(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> WorterbuchResult<Value> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Delete(
                transaction_id,
                self.interface.clone(),
                key,
                client_id,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn pdelete(
        &self,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        client_id: ClientId,
    ) -> WorterbuchResult<KeyValuePairs> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::PDelete(
                transaction_id,
                self.interface.clone(),
                pattern,
                client_id,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn connected(
        &self,
        client_id: ClientId,
        remote_addr: Option<SocketAddr>,
        protocol: Protocol,
    ) -> WorterbuchResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Connected(client_id, remote_addr, protocol, tx))
            .await?;
        rx.await?
    }

    async fn protocol_switched(
        &self,
        client_id: ClientId,
        protocol: ProtocolMajorVersion,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::ProtocolSwitched(
                client_id,
                self.interface.clone(),
                protocol,
            ))
            .await?;
        Ok(())
    }

    async fn disconnected(
        &self,
        client_id: ClientId,
        protocol: Protocol,
        remote_addr: Option<SocketAddr>,
    ) -> WorterbuchResult<()> {
        self.tx
            .send(WbFunction::Disconnected(client_id, protocol, remote_addr))
            .await?;
        Ok(())
    }

    async fn export(&self, span: Span) -> WorterbuchResult<(Value, GraveGoods, LastWill)> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(WbFunction::Export(tx, span)).await?;
        Ok(rx.await?)
    }

    async fn import(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        json: String,
    ) -> WorterbuchResult<Vec<(String, (ValueEntry, bool))>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WbFunction::Import(
                transaction_id,
                client_id,
                self.interface.clone(),
                json,
                tx,
            ))
            .await?;
        rx.await?
    }

    async fn entries(&self) -> WorterbuchResult<usize> {
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
