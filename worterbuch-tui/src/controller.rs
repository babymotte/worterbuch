use std::{fmt, net::SocketAddr, ops::ControlFlow};
use tokio::sync::mpsc;
use tosub::SubsystemHandle;
use totils::while_select;
use worterbuch_common::{ClientId, error::ConnectionError};

use crate::tui::Tui;

#[derive(Debug, Clone, Copy)]
pub enum Protocol {
    Tcp,
    Ws,
    Wss,
    Unix,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Protocol::Tcp => "tcp".fmt(f),
            Protocol::Ws => "ws".fmt(f),
            Protocol::Wss => "wss".fmt(f),
            Protocol::Unix => "unix".fmt(f),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ClientAddress {
    pub protocol: Protocol,
    pub socket: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct AppApi {
    tx: mpsc::Sender<ApiMessage>,
}

impl AppApi {
    pub fn new(subsys: &SubsystemHandle) -> (Self, mpsc::Receiver<UserAction>) {
        let (api_tx, api_rx) = mpsc::channel(32);
        let (client_address_tx, client_address_rx) = mpsc::channel(32);

        subsys.spawn("backend", |s| {
            AppActor::new(s, api_rx, client_address_tx).run()
        });

        (Self { tx: api_tx }, client_address_rx)
    }

    pub async fn connection_failed(&self, address: ClientAddress, e: ConnectionError) {
        self.tx
            .send(ApiMessage::ConnectionFailed(address, e))
            .await
            .ok();
    }

    pub async fn client_added(&self, address: ClientAddress, client_id: ClientId) {
        self.tx
            .send(ApiMessage::ClientAdded(address, client_id))
            .await
            .ok();
    }

    pub async fn client_disconnected(&self, client_id: ClientId) {
        self.tx
            .send(ApiMessage::ClientDisconnected(client_id))
            .await
            .ok();
    }
}

enum ApiMessage {
    ClientAdded(ClientAddress, ClientId),
    ConnectionFailed(ClientAddress, ConnectionError),
    ClientDisconnected(ClientId),
}

pub enum UserAction {
    CreateClient(ClientAddress),
    CloseClient(ClientId),
}

struct AppActor {
    subsys: SubsystemHandle,
    api_rx: mpsc::Receiver<ApiMessage>,
    tui: Tui,
}

impl AppActor {
    fn new(
        subsys: SubsystemHandle,
        api_rx: mpsc::Receiver<ApiMessage>,
        user_action_tx: mpsc::Sender<UserAction>,
    ) -> Self {
        let tui = Tui::new(user_action_tx);
        Self {
            subsys,
            api_rx,
            tui,
        }
    }

    async fn run(mut self) -> miette::Result<()> {
        while_select! {
            biased;
            _ = self.subsys.shutdown_requested() => break,
            recv = self.api_rx.recv() => self.process_api_message(recv).await,
        }

        Ok(())
    }

    async fn process_api_message(&mut self, recv: Option<ApiMessage>) -> ControlFlow<()> {
        let Some(msg) = recv else {
            return ControlFlow::Break(());
        };

        match msg {
            ApiMessage::ClientAdded(client_address, uuid) => {
                // TODO add new client view to TUI
            }
            ApiMessage::ConnectionFailed(client_address, connection_error) => {
                // TODO show error message in TUI reporting the failed connection attempt
            }
            ApiMessage::ClientDisconnected(uuid) => {
                // TODO show error message in TUI reporting the closed connection if client view is still open
            }
        }

        ControlFlow::Continue(())
    }
}
