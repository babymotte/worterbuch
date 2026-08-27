/*
 *  Worterbuch common modules library
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

#[cfg(feature = "benchmark")]
pub mod benchmark;

pub mod error;
pub mod protocol;

use crate::{
    error::{ConnectionError, ConnectionResult},
    protocol::v1::{
        CasVersion, GraveGoods, Key, KeyValuePair, KeyValuePairs, LastWill, LiveOnlyFlag,
        PStateEvent, ProtocolMajorVersion, ProtocolVersion, RequestPattern, SYSTEM_TOPIC_CLIENTS,
        SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_ROOT, SendTracesFlag,
        StateEvent, Trace, TransactionId, UniqueFlag, Value,
    },
};
use error::WorterbuchResult;
use hashbrown::HashMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    fmt::{self, Display},
    io,
    net::SocketAddr,
    ops::Deref,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWriteExt, BufReader, Lines},
    select,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tracing::{Span, debug, error, trace, warn};
use uuid::Uuid;

#[cfg(feature = "jemalloc")]
mod jemalloc;
#[cfg(feature = "jemalloc")]
pub mod profiling;
#[cfg(feature = "redb")]
pub mod redb;

pub const INTERNAL_CLIENT_ID: ClientId = ClientId::nil();

pub type ClientId = Uuid;

pub type TypedKeyValuePairs<T> = Vec<TypedKeyValuePair<T>>;
pub type Path = String;
pub type WorterbuchVersionSegment = u32;
pub type WorterbuchMajorVersion = WorterbuchVersionSegment;
pub type WorterbuchMinorVersion = WorterbuchVersionSegment;
pub type WorterbuchPatchVersion = WorterbuchVersionSegment;

pub type SubscriptionReceiver = mpsc::Receiver<(StateEvent, Option<Trace>)>;
pub type PSubscriptionReceiver = mpsc::Receiver<(PStateEvent, Option<Trace>)>;
pub type LsSubscriptionReceiver = mpsc::Receiver<(Vec<RegularKeySegment>, Option<Trace>)>;
pub type SubscriptionSender = mpsc::Sender<(StateEvent, Option<Trace>)>;
pub type PSubscriptionSender = mpsc::Sender<(PStateEvent, Option<Trace>)>;
pub type LsSubscriptionSender = mpsc::Sender<(Vec<RegularKeySegment>, Option<Trace>)>;

pub type Subscription = (SubscriptionReceiver, SubscriptionId);
pub type PSubscription = (PSubscriptionReceiver, SubscriptionId);
pub type LsSubscription = (LsSubscriptionReceiver, SubscriptionId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorterbuchVersion(
    pub WorterbuchMajorVersion,
    pub WorterbuchMinorVersion,
    pub WorterbuchPatchVersion,
);

impl WorterbuchVersion {
    #[cfg(feature = "commercial")]
    pub fn check_covered_by_license(
        &self,
        license_min: (WorterbuchVersionSegment, WorterbuchVersionSegment),
        license_max: (WorterbuchVersionSegment, WorterbuchVersionSegment),
    ) -> ConfigResult<()> {
        if license_min.0 == self.0 && license_min.1 > self.1 {
            return Err(ConfigError::InsufficientLicense(format!(
                "License is only valid for Wörterbuch versions {}.{} and later, but this is version {}.{}",
                license_min.0, license_min.1, self.0, self.1,
            )));
        }
        if license_max.0 == self.0 && license_max.1 <= self.1 {
            return Err(ConfigError::InsufficientLicense(format!(
                "License is only valid for Wörterbuch versions earlier than {}.{}, but this is version {}.{}",
                license_max.0, license_max.1, self.0, self.1,
            )));
        }
        if license_min.0 > self.0 {
            return Err(ConfigError::InsufficientLicense(format!(
                "License is only valid for Wörterbuch versions {}.{} and later, but this is version {}.{}",
                license_min.0, license_min.1, self.0, self.1,
            )));
        }
        if license_max.0 < self.0 {
            return Err(ConfigError::InsufficientLicense(format!(
                "License is only valid for Wörterbuch versions earlier than {}.{}, but this is version {}.{}",
                license_max.0, license_max.1, self.0, self.1,
            )));
        }

        Ok(())
    }
}

impl fmt::Display for WorterbuchVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum ValueEntry {
    Cas(Value, u64),
    #[serde(untagged)]
    Plain(Value),
}

impl AsRef<Value> for ValueEntry {
    fn as_ref(&self) -> &Value {
        match self {
            ValueEntry::Plain(value) => value,
            ValueEntry::Cas(value, _) => value,
        }
    }
}

impl From<ValueEntry> for Value {
    fn from(value: ValueEntry) -> Self {
        match value {
            ValueEntry::Plain(value) => value,
            ValueEntry::Cas(value, _) => value,
        }
    }
}

impl From<Value> for ValueEntry {
    fn from(value: Value) -> Self {
        ValueEntry::Plain(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Privilege {
    Read,
    Write,
    Delete,
    Profile,
    WebLogin,
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Privilege::Read => "read".fmt(f),
            Privilege::Write => "write".fmt(f),
            Privilege::Delete => "delete".fmt(f),
            Privilege::Profile => "profile".fmt(f),
            Privilege::WebLogin => "web-login".fmt(f),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthCheck<'a> {
    Pattern(&'a str),
    Flag,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthCheckOwned {
    Pattern(String),
    Flag,
}

impl<'a> From<AuthCheck<'a>> for AuthCheckOwned {
    fn from(value: AuthCheck<'a>) -> Self {
        match value {
            AuthCheck::Pattern(p) => AuthCheckOwned::Pattern(p.to_owned()),
            AuthCheck::Flag => AuthCheckOwned::Flag,
        }
    }
}

impl fmt::Display for AuthCheckOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthCheckOwned::Pattern(p) => p.fmt(f),
            AuthCheckOwned::Flag => true.fmt(f),
        }
    }
}

#[macro_export]
macro_rules! topic {
    ($first:expr $(, $rest:expr)*) => {{
        use std::fmt::Write;
        let mut s = String::new();
        write!(s, "{}", $first).expect("writing to a String never fails");
        $(
            s.push('/');
            write!(s, "{}", $rest).expect("writing to a String never fails");
        )*
        s
    }};
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Hash, Deserialize, JsonSchema)]
#[serde(rename_all = "UPPERCASE")]
pub enum Protocol {
    TCP,
    WS,
    HTTP,
    UNIX,
    Proxied(Box<Protocol>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedKeyValuePair<T: DeserializeOwned> {
    pub key: Key,
    pub value: T,
}

impl<T: DeserializeOwned> TryFrom<KeyValuePair> for TypedKeyValuePair<T> {
    type Error = serde_json::Error;

    fn try_from(kvp: KeyValuePair) -> Result<Self, Self::Error> {
        let deserialized = serde_json::from_value(kvp.value)?;
        Ok(TypedKeyValuePair {
            key: kvp.key,
            value: deserialized,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedPStateEvent<T: DeserializeOwned> {
    KeyValuePairs(TypedKeyValuePairs<T>),
    Deleted(TypedKeyValuePairs<T>),
}

impl<T: DeserializeOwned> TryFrom<PStateEvent> for TypedPStateEvent<T> {
    type Error = serde_json::Error;

    fn try_from(value: PStateEvent) -> Result<Self, Self::Error> {
        match value {
            PStateEvent::KeyValuePairs(kvps) => Ok(TypedPStateEvent::KeyValuePairs(
                try_to_typed_key_value_pairs(kvps)?,
            )),
            PStateEvent::Deleted(kvps) => Ok(TypedPStateEvent::Deleted(
                try_to_typed_key_value_pairs(kvps)?,
            )),
        }
    }
}

fn try_to_typed_key_value_pairs<T: DeserializeOwned>(
    kvps: KeyValuePairs,
) -> Result<TypedKeyValuePairs<T>, serde_json::Error> {
    let mut out = vec![];

    for kvp in kvps {
        out.push(kvp.try_into()?);
    }

    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedStateEvent<T: DeserializeOwned> {
    Value(T),
    Deleted(T),
}

impl<T: DeserializeOwned> From<TypedStateEvent<T>> for Option<T> {
    fn from(e: TypedStateEvent<T>) -> Self {
        match e {
            TypedStateEvent::Value(v) => Some(v),
            TypedStateEvent::Deleted(_) => None,
        }
    }
}

impl<T: DeserializeOwned> From<TypedKeyValuePair<T>> for TypedStateEvent<T> {
    fn from(kvp: TypedKeyValuePair<T>) -> Self {
        TypedStateEvent::Value(kvp.value)
    }
}

impl<T: DeserializeOwned + TryFrom<Value, Error = serde_json::Error>> TryFrom<StateEvent>
    for TypedStateEvent<T>
{
    type Error = serde_json::Error;

    fn try_from(e: StateEvent) -> Result<Self, Self::Error> {
        match e {
            StateEvent::Value(v) => Ok(TypedStateEvent::Value(v.try_into()?)),
            StateEvent::Deleted(v) => Ok(TypedStateEvent::Deleted(v.try_into()?)),
        }
    }
}

pub type TypedStateEvents<T> = Vec<TypedStateEvent<T>>;

pub type TypedPStateEvents<T> = Vec<TypedPStateEvent<T>>;

// #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord, Tags)]
pub type RegularKeySegment = String;

pub fn parse_segments(pattern: &str) -> WorterbuchResult<Vec<RegularKeySegment>> {
    let mut segments = Vec::new();
    for segment in pattern.split('/') {
        let ks: KeySegment = segment.into();
        match ks {
            KeySegment::Regular(reg) => segments.push(reg),
            KeySegment::Wildcard => {
                return Err(error::WorterbuchError::IllegalWildcard(pattern.to_owned()));
            }
            KeySegment::MultiWildcard => {
                return Err(error::WorterbuchError::IllegalMultiWildcard(
                    pattern.to_owned(),
                ));
            }
        }
    }
    Ok(segments)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeySegment {
    Regular(RegularKeySegment),
    Wildcard,
    MultiWildcard,
    // RegexWildcard(String),
}

impl AsRef<str> for KeySegment {
    fn as_ref(&self) -> &str {
        match self {
            KeySegment::Regular(segment) => segment.as_str(),
            KeySegment::Wildcard => "?",
            KeySegment::MultiWildcard => "#",
        }
    }
}

pub fn format_path(path: &[impl AsRef<str>]) -> String {
    let mut path = path.iter().fold(String::new(), |mut a, b| {
        let b = b.as_ref();
        a.reserve(b.len() + 1);
        a.push_str(b);
        a.push('/');
        a
    });
    path.pop();
    path
}

impl From<RegularKeySegment> for KeySegment {
    fn from(reg: RegularKeySegment) -> Self {
        Self::Regular(reg)
    }
}

impl Deref for KeySegment {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            KeySegment::Regular(reg) => reg,
            KeySegment::Wildcard => "?",
            KeySegment::MultiWildcard => "#",
        }
    }
}

impl fmt::Display for KeySegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeySegment::Regular(segment) => segment.fmt(f),
            KeySegment::Wildcard => write!(f, "?"),
            KeySegment::MultiWildcard => write!(f, "#"),
            // PathSegment::RegexWildcard(regex) => write!(f, "?{regex}?"),
        }
    }
}

impl From<&str> for KeySegment {
    fn from(str: &str) -> Self {
        match str {
            "?" => KeySegment::Wildcard,
            "#" => KeySegment::MultiWildcard,
            other => KeySegment::Regular(other.to_owned()),
        }
    }
}

impl KeySegment {
    pub fn parse(pattern: impl AsRef<str>) -> Vec<KeySegment> {
        let segments = pattern.as_ref().split('/');
        segments.map(KeySegment::from).collect()
    }
}

pub fn quote(str: impl AsRef<str>) -> String {
    let str_ref = str.as_ref();
    if str_ref.starts_with('\"') && str_ref.ends_with('\"') {
        str_ref.to_owned()
    } else {
        format!("\"{str_ref}\"")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SubscriptionId {
    pub client_id: ClientId,
    pub transaction_id: TransactionId,
}

impl SubscriptionId {
    pub fn new(client_id: ClientId, transaction_id: TransactionId) -> Self {
        SubscriptionId {
            client_id,
            transaction_id,
        }
    }
}

pub trait WbApi {
    fn supported_protocol_versions(&self) -> Box<[ProtocolVersion]>;

    fn version(&self) -> &str;

    fn get(&self, key: Key) -> impl Future<Output = WorterbuchResult<Value>> + Send;

    fn cget(&self, key: Key) -> impl Future<Output = WorterbuchResult<(Value, CasVersion)>> + Send;

    fn pget(
        &self,
        pattern: RequestPattern,
    ) -> impl Future<Output = WorterbuchResult<KeyValuePairs>> + Send;

    fn set(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn cset(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        version: CasVersion,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn acquire_lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<oneshot::Receiver<()>>> + Send;

    fn release_lock(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn spub_init(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn spub(
        &self,
        transaction_id: TransactionId,
        value: Value,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn publish(
        &self,
        transaction_id: TransactionId,
        key: Key,
        value: Value,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn ls(
        &self,
        parent: Option<Key>,
    ) -> impl Future<Output = WorterbuchResult<Vec<RegularKeySegment>>> + Send;

    fn pls(
        &self,
        parent: Option<RequestPattern>,
    ) -> impl Future<Output = WorterbuchResult<Vec<RegularKeySegment>>> + Send;

    fn subscribe(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        key: Key,
        unique: UniqueFlag,
        live_only: LiveOnlyFlag,
        send_traces: SendTracesFlag,
    ) -> impl Future<Output = WorterbuchResult<Subscription>> + Send;

    fn psubscribe(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        unique: UniqueFlag,
        live_only: LiveOnlyFlag,
        send_traces: SendTracesFlag,
    ) -> impl Future<Output = WorterbuchResult<PSubscription>> + Send;

    fn subscribe_ls(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        parent: Option<Key>,
        send_traces: SendTracesFlag,
    ) -> impl Future<Output = WorterbuchResult<LsSubscription>> + Send;

    fn unsubscribe(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn unsubscribe_ls(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn delete(
        &self,
        transaction_id: TransactionId,
        key: Key,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<Value>> + Send;

    fn pdelete(
        &self,
        transaction_id: TransactionId,
        pattern: RequestPattern,
        quiet: Option<bool>,
        client_id: ClientId,
    ) -> impl Future<Output = WorterbuchResult<KeyValuePairs>> + Send;

    fn connected(
        &self,
        client_id: ClientId,
        remote_addr: Option<SocketAddr>,
        protocol: Protocol,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn protocol_switched(
        &self,
        client_id: ClientId,
        protocol: ProtocolMajorVersion,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn disconnected(
        &self,
        client_id: ClientId,
        protocol: Protocol,
        remote_addr: Option<SocketAddr>,
    ) -> impl Future<Output = WorterbuchResult<()>> + Send;

    fn export(
        &self,
        span: Span,
    ) -> impl Future<
        Output = WorterbuchResult<(
            Value,
            HashMap<ClientId, GraveGoods>,
            HashMap<ClientId, LastWill>,
        )>,
    > + Send;

    fn import(
        &self,
        client_id: ClientId,
        transaction_id: TransactionId,
        json: String,
    ) -> impl Future<Output = WorterbuchResult<Vec<(String, (ValueEntry, bool))>>> + Send;

    fn entries(&self) -> impl Future<Output = WorterbuchResult<usize>> + Send;
}

pub async fn receive_msg<T: DeserializeOwned, R: AsyncRead + Unpin>(
    rx: &mut Lines<BufReader<R>>,
    timeout: Option<Duration>,
) -> ConnectionResult<Option<T>> {
    let read = if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, rx.next_line()).await
    } else {
        Ok(rx.next_line().await)
    };
    match read {
        Ok(Ok(None)) => {
            warn!("No data received, connection closed by remote peer");
            Ok(None)
        }
        Ok(Ok(Some(json))) => {
            debug!("Received message: {json}");
            let sm = serde_json::from_str(&json);
            if let Err(e) = &sm {
                error!("Error deserializing message '{json}': {e}")
            }
            Ok(sm?)
        }
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err(ConnectionError::Timeout(Box::new(
            "timeout while receiving message".to_owned(),
        ))),
    }
}

pub async fn write_line_and_flush<F, Fut>(
    mut shutdown_request: F,
    msg: impl Serialize,
    mut tx: impl AsyncWriteExt + Unpin,
    send_timeout: Option<Duration>,
    remote: impl Display,
) -> ConnectionResult<()>
where
    F: FnMut() -> Fut,
    Fut: IntoFuture<Output = ()>,
{
    let mut json = serde_json::to_string(&msg)?;
    if json.contains('\n') {
        return Err(ConnectionError::IoError(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid JSON: '{json}' contains line break"),
        ))));
    }
    if json.trim().is_empty() {
        return Err(ConnectionError::IoError(Box::new(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid JSON: '{json}' is empty"),
        ))));
    }

    json.push('\n');
    let bytes = json.as_bytes();

    debug!("Sending message with timeout {send_timeout:?}: {json}");
    trace!("Writing line …");
    for chunk in bytes.chunks(1024) {
        let mut written = 0;
        while written < chunk.len() {
            let do_write = tx.write(&chunk[written..]);
            let additionally_written = if let Some(send_timeout) = send_timeout {
                do_with_timeout(&mut shutdown_request, &remote, do_write, send_timeout).await??
            } else {
                do_without_timeout(&mut shutdown_request, do_write).await??
            };
            written += additionally_written;
        }
    }
    trace!("Writing line done.");
    trace!("Flushing channel …");

    let do_flush = tx.flush();

    if let Some(send_timeout) = send_timeout {
        do_with_timeout(&mut shutdown_request, &remote, do_flush, send_timeout).await??;
    } else {
        do_without_timeout(&mut shutdown_request, do_flush).await??;
    }
    trace!("Flushing channel done.");

    Ok(())
}

async fn do_without_timeout<F, Fut, T>(
    shutdown_request: &mut F,
    task: impl Future<Output = io::Result<T>>,
) -> ConnectionResult<io::Result<T>>
where
    F: FnMut() -> Fut,
    Fut: IntoFuture<Output = ()>,
{
    select! {
        biased;
        _ = shutdown_request() => {
            Err(ConnectionError::ShutdownRequested)
        },
        res = task => Ok(res),
    }
}

async fn do_with_timeout<F, Fut, T>(
    shutdown_request: &mut F,
    remote: &impl Display,
    task: impl Future<Output = io::Result<T>>,
    send_timeout: Duration,
) -> ConnectionResult<io::Result<T>>
where
    F: FnMut() -> Fut,
    Fut: IntoFuture<Output = ()>,
{
    let res = select! {
        biased;
        _ = shutdown_request() => {
            return Err(ConnectionError::ShutdownRequested);
        },
        res = timeout(send_timeout, task) => res,
    };

    res.map_err(|_| {
        ConnectionError::Timeout(Box::new(format!(
            "timeout while sending tcp message to {remote}"
        )))
    })
}

pub fn is_grave_goods_topic(key: &str) -> bool {
    let mut split = key.split('/');
    (
        Some(SYSTEM_TOPIC_ROOT),
        Some(SYSTEM_TOPIC_CLIENTS),
        Some(SYSTEM_TOPIC_GRAVE_GOODS),
        None,
    ) == (split.next(), split.next(), split.nth(1), split.next())
}

pub fn is_last_will_topic(key: &str) -> bool {
    let mut split = key.split('/');
    (
        Some(SYSTEM_TOPIC_ROOT),
        Some(SYSTEM_TOPIC_CLIENTS),
        Some(SYSTEM_TOPIC_LAST_WILL),
        None,
    ) == (split.next(), split.next(), split.nth(1), split.next())
}

pub fn is_client_sys_wildcard_topic(pattern: &str) -> Option<Uuid> {
    let mut split = pattern.split('/');

    let root = split.next();
    let clients = split.next();
    let client_id = split.next();
    let wildcard = split.next();
    let end = split.next();

    if (
        Some(SYSTEM_TOPIC_ROOT),
        Some(SYSTEM_TOPIC_CLIENTS),
        Some(KeySegment::MultiWildcard.as_ref()),
        None,
    ) == (root, clients, wildcard, end)
    {
        // Extract the UUID from the client_id segment if possible
        client_id.and_then(|id| Uuid::parse_str(id).ok())
    } else {
        None
    }
}

mod macros {

    #[macro_export]
    macro_rules! while_select {
        (biased; $($tokens:tt)*) => {
            '__while_select: loop {
                match ::tokio::select! { biased; $($tokens)* } {
                    ::std::ops::ControlFlow::Continue(_) => {}
                    ::std::ops::ControlFlow::Break(v) => break '__while_select v,
                }
            }
        };
        ($($tokens:tt)*) => {
            '__while_select: loop {
                match ::tokio::select! { $($tokens)* } {
                    ::std::ops::ControlFlow::Continue(_) => {}
                    ::std::ops::ControlFlow::Break(v) => break '__while_select v,
                }
            }
        };
    }

    #[cfg(test)]
    mod test {

        #![allow(clippy::as_conversions)]
        #![allow(clippy::unwrap_used)]

        #[tokio::test]
        async fn while_select_breaks_as_expected_on_control_flow() {
            use std::{ops::ControlFlow, time::Duration};
            use tokio::time::sleep;

            let mut fut_a = Box::pin(async { ControlFlow::Break::<&'static str>("hello") });
            let mut fut_b = Box::pin(async {
                sleep(Duration::from_secs(1)).await;
                ControlFlow::Break::<&'static str>("nein")
            });

            let res = while_select!(
                it = &mut fut_a => it,
                it = &mut fut_b => it,
            );

            assert_eq!("hello", res);
        }

        #[tokio::test]
        async fn while_select_biased_breaks_as_expected_on_control_flow() {
            use std::{ops::ControlFlow, time::Duration};
            use tokio::time::sleep;

            let mut fut_a = Box::pin(async { ControlFlow::Break::<&'static str>("hello") });
            let mut fut_b = Box::pin(async {
                sleep(Duration::from_secs(1)).await;
                ControlFlow::Break::<&'static str>("nein")
            });

            let res = while_select!(
                biased;
                it = &mut fut_a => it,
                it = &mut fut_b => it,
            );

            assert_eq!("hello", res);
        }

        #[tokio::test]
        async fn while_select_breaks_as_expected_on_break() {
            use std::{ops::ControlFlow, time::Duration};
            use tokio::time::sleep;

            let mut fut_a = Box::pin(async {});
            let mut fut_b = Box::pin(async {
                sleep(Duration::from_secs(1)).await;
                ControlFlow::Break::<&'static str>("nein")
            });

            let res = while_select!(
                _ = &mut fut_a => break "hello",
                it = &mut fut_b => it,
            );

            assert_eq!("hello", res);
        }
    }
}

#[cfg(test)]
mod test {

    #![allow(clippy::as_conversions)]
    #![allow(clippy::unwrap_used)]

    #[test]
    fn topic_macro_generates_topic_correctly() {
        assert_eq!(
            "hello/world/foo/bar",
            topic!("hello", "world", "foo", "bar")
        );
    }

    #[cfg(feature = "commercial")]
    mod commercial {

        use crate::WorterbuchVersion;

        #[test]
        fn major_version_too_old_for_license_is_rejected() {
            let license_min = (2, 0);
            let license_max = (3, 0);
            assert!(
                WorterbuchVersion(0, 0, 1)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );

            assert!(
                WorterbuchVersion(1, 5, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
        }

        #[test]
        fn minor_version_too_old_for_license_is_rejected() {
            let license_min = (2, 6);
            let license_max = (3, 0);
            assert!(
                WorterbuchVersion(2, 0, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
            assert!(
                WorterbuchVersion(2, 5, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
            assert!(
                WorterbuchVersion(2, 5, 9999)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
        }

        #[test]
        fn major_version_too_new_for_license_is_rejected() {
            let license_min = (2, 0);
            let license_max = (3, 0);
            assert!(
                WorterbuchVersion(4, 5, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
            assert!(
                WorterbuchVersion(9999, 9999, 9999)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
        }

        #[test]
        fn minor_version_too_new_for_license_is_rejected() {
            let license_min = (2, 0);
            let license_max = (2, 6);
            assert!(
                WorterbuchVersion(2, 7, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
        }

        #[test]
        fn major_version_in_range_is_accepted() {
            let license_min = (1, 0);
            let license_max = (3, 0);
            assert!(
                WorterbuchVersion(1, 1, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_ok()
            );
            assert!(
                WorterbuchVersion(2, 0, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_ok()
            );
            assert!(
                WorterbuchVersion(2, 5, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_ok()
            );
            assert!(
                WorterbuchVersion(2, 99999, 99999)
                    .check_covered_by_license(license_min, license_max)
                    .is_ok()
            );
        }

        #[test]
        fn minor_version_in_range_is_accepted() {
            let license_min = (2, 1);
            let license_max = (2, 6);
            assert!(
                WorterbuchVersion(2, 5, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_ok()
            );
        }

        #[test]
        fn version_equal_to_min_version_is_accepted() {
            let license_min = (2, 1);
            let license_max = (2, 6);
            assert!(
                WorterbuchVersion(2, 1, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_ok()
            );
            assert!(
                WorterbuchVersion(2, 1, 5)
                    .check_covered_by_license(license_min, license_max)
                    .is_ok()
            );
        }

        #[test]
        fn version_equal_to_max_version_is_rejected() {
            let license_min = (1, 0);
            let license_max = (2, 6);
            assert!(
                WorterbuchVersion(2, 6, 0)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
            assert!(
                WorterbuchVersion(2, 6, 1)
                    .check_covered_by_license(license_min, license_max)
                    .is_err()
            );
        }
    }
}
