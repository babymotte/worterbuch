use crate::controller::{ClientAddress, Protocol, TuiApi, UserAction};
use std::{collections::HashMap, ops::ControlFlow};
use tokio::{spawn, sync::mpsc, task::JoinHandle};
use tosub::SubsystemHandle;
use totils::while_select;
use worterbuch_client::{Worterbuch, config::Config};
use worterbuch_common::{
    ClientId,
    protocol::v1::{Key, TransactionId},
};

/// A connected client together with its metadata and active subscriptions.
struct ClientHandle {
    wb: Worterbuch,
    address: ClientAddress,
    subscriptions: HashMap<TransactionId, JoinHandle<()>>,
    name: Option<String>,
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        for (_, task) in self.subscriptions.drain() {
            task.abort();
        }
    }
}

struct BackendActor {
    subsys: SubsystemHandle,
    tui: TuiApi,
    clients: HashMap<ClientId, ClientHandle>,
    user_actions: mpsc::Receiver<UserAction>,
}

impl BackendActor {
    fn new(subsys: SubsystemHandle, tui: TuiApi, user_actions: mpsc::Receiver<UserAction>) -> Self {
        Self {
            subsys,
            tui,
            clients: HashMap::new(),
            user_actions,
        }
    }

    async fn run(mut self) -> miette::Result<()> {
        while_select! {
            biased;
            _ = self.subsys.shutdown_requested() => break,
            recv = self.user_actions.recv() => self.process_user_action(recv).await,
        }

        Ok(())
    }

    async fn process_user_action(&mut self, recv: Option<UserAction>) -> ControlFlow<()> {
        let Some(action) = recv else {
            return ControlFlow::Break(());
        };

        match action {
            UserAction::CreateClient {
                protocol,
                address,
                name,
            } => self.add_client(protocol, address, name).await,
            UserAction::CloseClient(client_id) => self.close_client(client_id),
            UserAction::Reconnect { client } => self.reconnect(client).await,
            UserAction::Get { client, key } => self.get(client, key).await,
            UserAction::Set { client, key, value } => self.set(client, key, value).await,
            UserAction::Subscribe {
                client,
                key,
                unique,
                live_only,
            } => self.subscribe(client, key, unique, live_only).await,
            UserAction::Unsubscribe { client, sub } => self.unsubscribe(client, sub).await,
        }

        ControlFlow::Continue(())
    }

    async fn add_client(&mut self, protocol: Protocol, address: String, name: Option<String>) {
        let config = match build_config(protocol, &address) {
            Some(config) => config,
            None => {
                self.tui
                    .connection_failed(protocol, address, UNIX_UNSUPPORTED.to_owned())
                    .await;
                return;
            }
        };
        let client_address = ClientAddress { protocol, address };

        let (client, on_disconnect) = match worterbuch_client::connect(config).await {
            Ok(it) => it,
            Err(e) => {
                self.tui
                    .connection_failed(protocol, client_address.address, e.to_string())
                    .await;
                return;
            }
        };

        let client_id = client.client_id();

        let tui = self.tui.clone();
        spawn(async move {
            on_disconnect.await;
            tui.client_disconnected(client_id).await;
        });

        if let Some(name) = name.clone() {
            client.set_client_name(name).await.ok();
        }

        self.clients.insert(
            client_id,
            ClientHandle {
                wb: client,
                address: client_address.clone(),
                subscriptions: HashMap::new(),
                name: name.clone(),
            },
        );

        self.tui.client_added(client_address, client_id, name).await;
    }

    fn close_client(&mut self, client_id: ClientId) {
        // Dropping the handle aborts subscription tasks and disconnects the client
        // once the last `Worterbuch` clone is gone.
        self.clients.remove(&client_id);
    }

    async fn reconnect(&mut self, client_id: ClientId) {
        let Some(old) = self.clients.remove(&client_id) else {
            return;
        };
        let address = old.address.clone();
        let name = old.name.clone();
        // Drop the old handle first, closing its socket and aborting its
        // subscription tasks before we dial again.
        drop(old);

        let Some(config) = build_config(address.protocol, &address.address) else {
            self.tui
                .action_failed(client_id, UNIX_UNSUPPORTED.to_owned())
                .await;
            return;
        };
        let (client, on_disconnect) = match worterbuch_client::connect(config).await {
            Ok(it) => it,
            Err(e) => {
                self.tui.action_failed(client_id, e.to_string()).await;
                return;
            }
        };

        if let Some(name) = name.clone() {
            client.set_client_name(name).await.ok();
        }

        let new_client_id = client.client_id();

        let tui = self.tui.clone();
        spawn(async move {
            on_disconnect.await;
            tui.client_disconnected(new_client_id).await;
        });

        self.clients.insert(
            new_client_id,
            ClientHandle {
                wb: client,
                address: address.clone(),
                subscriptions: HashMap::new(),
                name,
            },
        );

        self.tui
            .client_reconnected(client_id, new_client_id, address)
            .await;
    }

    async fn get(&mut self, client_id: ClientId, key: Key) {
        let Some(handle) = self.clients.get(&client_id) else {
            return;
        };
        match handle.wb.get_generic(key.clone()).await {
            Ok(value) => self.tui.get_result(client_id, key, value).await,
            Err(e) => self.tui.action_failed(client_id, e.to_string()).await,
        }
    }

    async fn set(&mut self, client_id: ClientId, key: Key, raw: String) {
        let Some(handle) = self.clients.get(&client_id) else {
            return;
        };
        // Accept JSON if it parses, otherwise treat the input as a plain string.
        let value = serde_json::from_str(&raw).unwrap_or(serde_json::Value::String(raw));
        match handle.wb.set_generic(key.clone(), value).await {
            Ok(()) => self.tui.set_ok(client_id, key).await,
            Err(e) => self.tui.action_failed(client_id, e.to_string()).await,
        }
    }

    async fn subscribe(&mut self, client_id: ClientId, key: Key, unique: bool, live_only: bool) {
        let Some(handle) = self.clients.get_mut(&client_id) else {
            return;
        };

        let (mut events, tid) = match handle
            .wb
            .subscribe_generic(key.clone(), unique, live_only, false)
            .await
        {
            Ok(it) => it,
            Err(e) => {
                self.tui.action_failed(client_id, e.to_string()).await;
                return;
            }
        };

        let tui = self.tui.clone();
        let task = spawn(async move {
            while let Some(value) = events.recv().await {
                tui.subscription_event(client_id, tid, value).await;
            }
            tui.subscription_stopped(client_id, tid).await;
        });

        handle.subscriptions.insert(tid, task);
        self.tui.subscription_started(client_id, tid, key).await;
    }

    async fn unsubscribe(&mut self, client_id: ClientId, sub: TransactionId) {
        let Some(handle) = self.clients.get_mut(&client_id) else {
            return;
        };
        if let Some(task) = handle.subscriptions.remove(&sub) {
            task.abort();
        }
        if let Err(e) = handle.wb.unsubscribe(sub).await {
            self.tui.action_failed(client_id, e.to_string()).await;
        }
        self.tui.subscription_stopped(client_id, sub).await;
    }
}

const UNIX_UNSUPPORTED: &str = "unix sockets are not supported on this platform";

/// Build a client [`Config`] for the given protocol and address. Returns `None`
/// only for `unix` on a non-unix platform.
fn build_config(protocol: Protocol, address: &str) -> Option<Config> {
    match protocol {
        Protocol::Tcp | Protocol::Ws | Protocol::Wss => {
            Some(Config::with_servers(protocol.to_string(), [address]))
        }
        Protocol::Unix => unix_config(address),
    }
}

#[cfg(target_family = "unix")]
fn unix_config(path: &str) -> Option<Config> {
    let mut config = Config::new();
    config.proto = "unix".to_owned();
    config.socket_path = Some(std::path::PathBuf::from(path));
    Some(config)
}

#[cfg(not(target_family = "unix"))]
fn unix_config(_path: &str) -> Option<Config> {
    None
}

pub async fn start(subsys: SubsystemHandle) -> miette::Result<()> {
    let (tui, user_actions) = TuiApi::new(&subsys);
    BackendActor::new(subsys, tui, user_actions).run().await
}
