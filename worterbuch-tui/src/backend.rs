use crate::controller::{AppApi, ClientAddress, UserAction};
use std::{collections::HashMap, ops::ControlFlow};
use tokio::{spawn, sync::mpsc};
use tosub::SubsystemHandle;
use totils::while_select;
use worterbuch_client::{Worterbuch, config::Config};
use worterbuch_common::ClientId;

struct BackendActor {
    subsys: SubsystemHandle,
    tui: AppApi,
    clients: HashMap<ClientId, Worterbuch>,
    user_actions: mpsc::Receiver<UserAction>,
}

impl BackendActor {
    fn new(subsys: SubsystemHandle, tui: AppApi, user_actions: mpsc::Receiver<UserAction>) -> Self {
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
            UserAction::CreateClient(address) => self.add_client(address).await,
            UserAction::CloseClient(client_id) => self.close_client(client_id).await,
        }

        ControlFlow::Continue(())
    }

    async fn add_client(&mut self, address: ClientAddress) {
        let proto = address.protocol.to_string();
        let servers = Box::new([address.socket]);

        let config = Config::with_servers(proto, servers);
        let (client, on_disconnect) = match worterbuch_client::connect(config).await {
            Ok(it) => it,
            Err(e) => {
                self.tui.connection_failed(address, e);
                return;
            }
        };

        let tui = self.tui.clone();
        let client_id = client.client_id().to_owned();

        spawn(async move {
            on_disconnect.await;
            tui.client_disconnected(client_id);
        });

        self.tui.client_added(address, client.client_id());

        self.clients.insert(client_id, client);
    }

    async fn close_client(&mut self, client_id: ClientId) {
        self.clients.remove(&client_id);
    }
}

pub async fn start(subsys: SubsystemHandle) -> miette::Result<()> {
    let (tui, user_actions) = AppApi::new(&subsys);
    BackendActor::new(subsys, tui, user_actions).run().await
}
