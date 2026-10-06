use crate::{
    Config,
    auth::JwtClaims,
    cluster::protocol::{Connected, Disconnected, ProxyMessage, Request, locks::Locks},
    server::common::{
        self, CloneableWbApi,
        protocol::{Proto, ServerMessageLazyBroadcaster},
    },
};
use hashbrown::HashMap;
use miette::{Context, IntoDiagnostic, bail};
use serde_json::json;
use std::{
    io::{self},
    net::SocketAddr,
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};
use tosub::Subsystem;
use totils::while_select;
use tracing::{Level, debug, enabled, error, info, trace, warn};
use worterbuch_common::{
    ClientId, Protocol, WbApi,
    protocol::v1::{
        ClientMessage, GraveGoods, Interface, LastWill, SYSTEM_TOPIC_CLIENTS,
        SYSTEM_TOPIC_GRAVE_GOODS, SYSTEM_TOPIC_LAST_WILL, SYSTEM_TOPIC_ROOT, ServerMessage,
    },
    topic,
};

pub struct VirtualProxyClientHandler {
    authorized: Option<JwtClaims>,
    proto: Proto,
    disconnect_handled: Arc<AtomicBool>,
}

pub enum VirtualServerMessage {
    ServerMessage((ClientId, ServerMessage)),
    Disconnect(ClientId),
}

pub struct VirtualProxyServer {
    pub subsys: Subsystem,
    pub clients: HashMap<ClientId, VirtualProxyClientHandler>,
    pub worterbuch: CloneableWbApi,
    pub config: Config,
    pub proxy_address: SocketAddr,
    pub send_tx: mpsc::Sender<VirtualServerMessage>,
}

impl VirtualProxyServer {
    pub async fn process_proxy_message(
        &mut self,
        recv: io::Result<Option<String>>,
        follower: SocketAddr,
    ) -> miette::Result<ControlFlow<()>> {
        trace!(enter = "process_proxy_message");
        match recv
            .into_diagnostic()
            .wrap_err_with(|| format!("follower/proxy {follower} closed the connection"))?
        {
            Some(line) => {
                debug!("Received message from proxy: {line}");

                self.process_line(line)
                    .await
                    .wrap_err("could not process proxy message")?;
                trace!(exit = "process_proxy_message");
                Ok(ControlFlow::Continue(()))
            }
            None => {
                info!("Follower/proxy {follower} closed the connection.");
                trace!(exit = "process_proxy_message");
                Ok(ControlFlow::Break(()))
            }
        }
    }

    async fn process_line(&mut self, line: String) -> miette::Result<()> {
        trace!(enter = "process_line", line);
        let msg: ProxyMessage = serde_json::from_str(&line)
            .into_diagnostic()
            .wrap_err("could not parse proxy message")?;

        match msg {
            ProxyMessage::Handshake(_) => {
                bail!("received handshake message from proxy after initial handshake");
            }
            ProxyMessage::Connected(Connected {
                client_id,
                protocol,
            }) => {
                self.spawn_virtual_client(
                    client_id,
                    protocol,
                    self.config.clone(),
                    self.worterbuch.named(format!("client/{client_id}")),
                )
                .await?;
            }
            ProxyMessage::Disconnected(Disconnected {
                client_id,
                protocol,
                grave_goods,
                last_will,
            }) => {
                self.apply_grave_goods_and_last_will(client_id, grave_goods, last_will)
                    .await?;
                self.stop_virtual_client(client_id, protocol).await?;
            }
            ProxyMessage::Request(Request {
                client_id,
                msg,
                interface,
            }) => {
                let processed = self
                    .process_client_request(client_id, msg, interface)
                    .await?;
                if !processed {
                    self.send_tx
                        .send(VirtualServerMessage::Disconnect(client_id))
                        .await
                        .ok();
                }
            }
        }

        trace!(exit = "process_line");

        Ok(())
    }

    pub async fn spawn_virtual_client(
        &mut self,
        client_id: ClientId,
        protocol: Protocol,
        config: Config,
        worterbuch: CloneableWbApi,
    ) -> miette::Result<ServerMessageLazyBroadcaster> {
        trace!(enter = "spawn_virtual_client", %client_id);
        let proxied_protocol = Protocol::Proxied(Box::new(protocol.clone()));

        // register the client before anything else so that the registration is guaranteed to be processed
        // before any request or disconnect of this client; on error (e.g. a client ID collision with a stale
        // connection of the same proxy) nothing has been spawned yet, so the existing registration is left untouched
        worterbuch
            .connected(client_id, None, proxied_protocol.clone())
            .await
            .into_diagnostic()
            .wrap_err_with(|| {
                format!(
                    "could not register proxied client {client_id} ({}/{:?})",
                    self.proxy_address, protocol
                )
            })?;

        let auth_required = config.auth_token_key.is_some();
        let (send_client_tx, send_client_rx) = mpsc::channel(config.channel_buffer_size);
        let disconnect_handled = Arc::new(AtomicBool::new(false));

        let send_tx = self.send_tx.clone();
        let wb = worterbuch.named(client_id);
        let proxy_addr = self.proxy_address;
        let dh = disconnect_handled.clone();
        self.subsys
            .spawn("leader-response-forwarder", move |s| async move {
                response_forwarder_loop(s, send_client_rx, send_tx, client_id, proxy_addr).await;

                if dh.swap(true, Ordering::AcqRel) {
                    debug!("Disconnect of client {client_id} has already been handled.");
                    return Ok(());
                }

                info!("Client {client_id} disconnected.");
                wb.disconnected(client_id, proxied_protocol, None)
                    .await
                    .wrap_err("could not notify core system about client disconnect")
            });

        let proto = Proto::new(
            self.subsys.clone(),
            client_id,
            send_client_tx.clone(),
            auth_required,
            config,
            worterbuch,
        );

        let client = VirtualProxyClientHandler {
            authorized: None,
            proto,
            disconnect_handled,
        };

        self.clients.insert(client_id, client);

        info!(
            "New proxied client connected: {} ({}/{:?})",
            client_id, self.proxy_address, protocol
        );

        trace!(exit = "spawn_virtual_client");
        Ok(send_client_tx)
    }

    async fn stop_virtual_client(
        &mut self,
        client_id: ClientId,
        protocol: Protocol,
    ) -> miette::Result<()> {
        trace!(enter = "stop_virtual_client", %client_id);
        debug!("Stopping virtual client {client_id} …");
        let Some(client) = self.clients.remove(&client_id) else {
            warn!(
                "Received disconnect for unknown client {client_id} ({}/{:?})",
                self.proxy_address, protocol
            );
            trace!(exit = "stop_virtual_client");
            return Ok(());
        };

        if client.disconnect_handled.swap(true, Ordering::AcqRel) {
            debug!("Disconnect of virtual client {client_id} has already been handled.");
            trace!(exit = "stop_virtual_client");
            return Ok(());
        }

        debug!(
            "Virtual client {client_id} removed from local register, triggering client disconnect callback …"
        );

        self.worterbuch
            .disconnected(
                client_id,
                Protocol::Proxied(Box::new(protocol.clone())),
                None,
            )
            .await?;

        info!(
            "Proxied client disconnected: {} ({}/{:?})",
            client_id, self.proxy_address, protocol
        );

        trace!(exit = "stop_virtual_client");
        Ok(())
    }

    async fn process_client_request(
        &mut self,
        client_id: ClientId,
        msg: ClientMessage,
        interface: Interface,
    ) -> miette::Result<bool> {
        trace!(enter = "process_client_request", %client_id, ?msg);
        let Some(client_handler) = self.clients.get_mut(&client_id) else {
            bail!(
                "Received request for unknown client {client_id} ({}/{:?})",
                self.proxy_address,
                interface
            );
        };

        let msg_processed = client_handler
            .proto
            .process_client_message(msg, &mut client_handler.authorized)
            .await?;

        trace!(exit = "process_client_request", %client_id);
        Ok(msg_processed)
    }

    pub async fn register_clients(
        &mut self,
        connected_clients: &[Connected],
    ) -> miette::Result<HashMap<ClientId, ServerMessageLazyBroadcaster>> {
        trace!(enter = "register_clients");
        let mut client_txs = HashMap::new();

        for Connected {
            client_id,
            protocol,
        } in connected_clients
        {
            let tx = self
                .spawn_virtual_client(
                    *client_id,
                    protocol.clone(),
                    self.config.clone(),
                    self.worterbuch.named(format!("client/{client_id}")),
                )
                .await?;
            client_txs.insert(*client_id, tx);
        }

        trace!(exit = "register_clients");
        Ok(client_txs)
    }

    pub async fn restore_locks(&self, locks: Locks) -> miette::Result<common::UpdatedLocks> {
        trace!(enter = "restore_locks");
        let updated_locks = self
            .worterbuch
            .re_grant_locks(locks.clone())
            .await
            .wrap_err("error while trying to re-grant previously held locks")?;

        for (client_id, keys) in &updated_locks.lost {
            if enabled!(Level::DEBUG) {
                let lost_keys: Vec<_> = keys.iter().map(|(_, key)| key).collect();
                debug!(
                    "Client {client_id} lost locks on keys {:?} after reconnecting to leader.",
                    lost_keys
                );
            }
        }

        for (client_id, keys) in &updated_locks.held {
            if enabled!(Level::DEBUG) {
                let held_keys: Vec<_> = keys.iter().map(|(_, key)| key).collect();
                debug!(
                    "Client {client_id} still holds locks on keys {:?} after reconnecting to leader.",
                    held_keys
                );
            }
        }

        for (client_id, pending_locks) in &updated_locks.pending {
            if enabled!(Level::DEBUG) {
                let pending_keys: Vec<_> = pending_locks.iter().map(|(_, key, _, _)| key).collect();
                debug!(
                    "Client {client_id} is still waiting for lock on keys {:?} after reconnecting to leader.",
                    pending_keys
                );
            }
        }

        trace!(exit = "restore_locks");
        Ok(updated_locks)
    }

    async fn apply_grave_goods_and_last_will(
        &self,
        client_id: ClientId,
        grave_goods: GraveGoods,
        last_will: LastWill,
    ) -> miette::Result<()> {
        trace!(enter = "apply_grave_goods_and_last_will", %client_id, ?grave_goods, ?last_will);
        self.worterbuch
            .set(
                0,
                topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_GRAVE_GOODS
                ),
                json!(grave_goods),
                client_id,
            )
            .await?;

        self.worterbuch
            .set(
                0,
                topic!(
                    SYSTEM_TOPIC_ROOT,
                    SYSTEM_TOPIC_CLIENTS,
                    client_id,
                    SYSTEM_TOPIC_LAST_WILL
                ),
                json!(last_will),
                client_id,
            )
            .await?;

        trace!(exit = "apply_grave_goods_and_last_will");
        Ok(())
    }
}

async fn response_forwarder_loop(
    subsys: Subsystem,
    mut send_client_rx: mpsc::Receiver<oneshot::Receiver<ServerMessage>>,
    send_tx: mpsc::Sender<VirtualServerMessage>,
    client_id: uuid::Uuid,
    proxy_addr: SocketAddr,
) {
    trace!(enter = "response_forwarder_loop", %client_id);

    while_select! {
        biased;
        _ = subsys.shutdown_requested() => break,
        recv = send_client_rx.recv() => forward_leader_response(recv, &send_tx, client_id, proxy_addr).await,
    }

    trace!(exit = "response_forwarder_loop");
}

async fn forward_leader_response(
    recv: Option<oneshot::Receiver<ServerMessage>>,
    send_tx: &mpsc::Sender<VirtualServerMessage>,
    client_id: ClientId,
    remote_addr: SocketAddr,
) -> ControlFlow<()> {
    trace!(enter = "forward_leader_response", %client_id);
    match recv {
        Some(msg) => match msg.await {
            Ok(msg) => match send_tx
                .send(VirtualServerMessage::ServerMessage((client_id, msg)))
                .await
                .into_diagnostic()
                .wrap_err("could not forward response to proxy")
            {
                Ok(_) => ControlFlow::Continue(()),
                Err(e) => {
                    error!("Failed to forward response to proxy: {:?}", e);
                    error!("Closing connection to proxy {}", remote_addr);
                    trace!(exit = "forward_leader_response");
                    ControlFlow::Break(())
                }
            },
            Err(_) => {
                trace!(exit = "forward_leader_response");
                ControlFlow::Break(())
            }
        },
        None => {
            trace!(exit = "forward_leader_response");
            ControlFlow::Break(())
        }
    }
}
