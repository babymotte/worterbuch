use crate::tui;
use std::{fmt, net::SocketAddr};
use tokio::sync::mpsc;
use tosub::SubsystemHandle;
use worterbuch_common::{
    ClientId,
    protocol::v1::{Key, TransactionId, Value},
};

/// Transport protocol used to reach a Wörterbuch server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Tcp,
    Ws,
    Wss,
}

impl Protocol {
    pub const ALL: [Protocol; 3] = [Protocol::Tcp, Protocol::Ws, Protocol::Wss];

    pub fn next(self) -> Self {
        match self {
            Protocol::Tcp => Protocol::Ws,
            Protocol::Ws => Protocol::Wss,
            Protocol::Wss => Protocol::Tcp,
        }
    }

    pub fn prev(self) -> Self {
        match self {
            Protocol::Tcp => Protocol::Wss,
            Protocol::Ws => Protocol::Tcp,
            Protocol::Wss => Protocol::Ws,
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Protocol::Tcp => "tcp".fmt(f),
            Protocol::Ws => "ws".fmt(f),
            Protocol::Wss => "wss".fmt(f),
        }
    }
}

/// A resolved server address a client is connected to.
#[derive(Debug, Clone, Copy)]
pub struct ClientAddress {
    pub protocol: Protocol,
    pub socket: SocketAddr,
}

impl fmt::Display for ClientAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.protocol, self.socket)
    }
}

/// Handle the backend uses to push updates into the TUI.
#[derive(Debug, Clone)]
pub struct TuiApi {
    tx: mpsc::Sender<TuiMessage>,
}

impl TuiApi {
    /// Spawn the TUI subsystem and return a handle for pushing updates to it,
    /// together with the receiver over which the backend gets user actions.
    pub fn new(subsys: &SubsystemHandle) -> (Self, mpsc::Receiver<UserAction>) {
        let (msg_tx, msg_rx) = mpsc::channel(256);
        let (action_tx, action_rx) = mpsc::channel(256);

        subsys.spawn("tui", move |s| tui::run(s, msg_rx, action_tx));

        (Self { tx: msg_tx }, action_rx)
    }

    async fn send(&self, msg: TuiMessage) {
        self.tx.send(msg).await.ok();
    }

    pub async fn connection_failed(&self, protocol: Protocol, address: String, error: String) {
        self.send(TuiMessage::ConnectionFailed {
            protocol,
            address,
            error,
        })
        .await;
    }

    pub async fn client_added(&self, address: ClientAddress, client_id: ClientId) {
        self.send(TuiMessage::ClientAdded { address, client_id })
            .await;
    }

    pub async fn client_disconnected(&self, client_id: ClientId) {
        self.send(TuiMessage::ClientDisconnected { client_id })
            .await;
    }

    pub async fn client_reconnected(
        &self,
        old_client_id: ClientId,
        client_id: ClientId,
        address: ClientAddress,
    ) {
        self.send(TuiMessage::ClientReconnected {
            old_client_id,
            client_id,
            address,
        })
        .await;
    }

    pub async fn action_failed(&self, client_id: ClientId, error: String) {
        self.send(TuiMessage::ActionFailed { client_id, error })
            .await;
    }

    pub async fn get_result(&self, client_id: ClientId, key: Key, value: Option<Value>) {
        self.send(TuiMessage::GetResult {
            client_id,
            key,
            value,
        })
        .await;
    }

    pub async fn set_ok(&self, client_id: ClientId, key: Key) {
        self.send(TuiMessage::SetOk { client_id, key }).await;
    }

    pub async fn subscription_started(&self, client_id: ClientId, sub: TransactionId, key: Key) {
        self.send(TuiMessage::SubscriptionStarted {
            client_id,
            sub,
            key,
        })
        .await;
    }

    pub async fn subscription_event(
        &self,
        client_id: ClientId,
        sub: TransactionId,
        value: Option<Value>,
    ) {
        self.send(TuiMessage::SubscriptionEvent {
            client_id,
            sub,
            value,
        })
        .await;
    }

    pub async fn subscription_stopped(&self, client_id: ClientId, sub: TransactionId) {
        self.send(TuiMessage::SubscriptionStopped { client_id, sub })
            .await;
    }
}

/// Updates sent from the backend to the TUI.
#[derive(Debug)]
pub enum TuiMessage {
    ClientAdded {
        address: ClientAddress,
        client_id: ClientId,
    },
    ConnectionFailed {
        protocol: Protocol,
        address: String,
        error: String,
    },
    ClientDisconnected {
        client_id: ClientId,
    },
    ClientReconnected {
        old_client_id: ClientId,
        client_id: ClientId,
        address: ClientAddress,
    },
    ActionFailed {
        client_id: ClientId,
        error: String,
    },
    GetResult {
        client_id: ClientId,
        key: Key,
        value: Option<Value>,
    },
    SetOk {
        client_id: ClientId,
        key: Key,
    },
    SubscriptionStarted {
        client_id: ClientId,
        sub: TransactionId,
        key: Key,
    },
    SubscriptionEvent {
        client_id: ClientId,
        sub: TransactionId,
        value: Option<Value>,
    },
    SubscriptionStopped {
        client_id: ClientId,
        sub: TransactionId,
    },
}

/// Actions the user triggers in the TUI, handled by the backend.
#[derive(Debug)]
pub enum UserAction {
    CreateClient {
        protocol: Protocol,
        address: String,
    },
    CloseClient(ClientId),
    Reconnect {
        client: ClientId,
    },
    Get {
        client: ClientId,
        key: Key,
    },
    Set {
        client: ClientId,
        key: Key,
        value: String,
    },
    Subscribe {
        client: ClientId,
        key: Key,
        unique: bool,
        live_only: bool,
    },
    Unsubscribe {
        client: ClientId,
        sub: TransactionId,
    },
}
