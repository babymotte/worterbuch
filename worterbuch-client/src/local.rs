/*
 *  Worterbuch local client module
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

use serde_json::json;
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
};
use worterbuch_common::{
    INTERNAL_CLIENT_ID, LsSubscriptionReceiver, PSubscriptionReceiver, Protocol,
    SubscriptionReceiver, WbApi,
    error::{ConnectionResult, WorterbuchError},
    protocol::v1::{
        Ack, CState, CStateEvent, ClientInfo, ClientMessage, Err, ErrorCode, LsState, PState,
        PStateEvent, RequestPattern, ServerInfo, ServerMessage, State, StateEvent, TransactionId,
        Welcome,
    },
};

pub struct LocalClientSocket {
    tx: mpsc::UnboundedSender<ClientMessage>,
    rx: mpsc::UnboundedReceiver<ServerMessage>,
    closed: oneshot::Receiver<()>,
}

impl LocalClientSocket {
    pub fn new(
        tx: mpsc::UnboundedSender<ClientMessage>,
        rx: mpsc::UnboundedReceiver<ServerMessage>,
        closed: oneshot::Receiver<()>,
    ) -> Self {
        Self { tx, rx, closed }
    }

    pub async fn send_msg(&self, msg: ClientMessage) -> ConnectionResult<()> {
        self.tx.send(msg)?;
        Ok(())
    }

    pub async fn receive_msg(&mut self) -> ConnectionResult<Option<ServerMessage>> {
        Ok(self.rx.recv().await)
    }

    pub async fn close(self) -> ConnectionResult<()> {
        drop(self.tx);
        drop(self.rx);
        let _ = self.closed.await;
        Ok(())
    }

    pub fn spawn_api_forward_loop(
        api: impl WbApi + Send + Sync + 'static,
        crx: mpsc::UnboundedReceiver<ClientMessage>,
        stx: mpsc::UnboundedSender<ServerMessage>,
    ) {
        let future = forward_loop(api, crx, stx);
        spawn(future);
    }
}

async fn forward_loop(
    api: impl WbApi + Send + Sync + 'static,
    mut crx: mpsc::UnboundedReceiver<ClientMessage>,
    stx: mpsc::UnboundedSender<ServerMessage>,
) {
    let spv = api.supported_protocol_versions();
    let version = api.version().to_owned();
    let welcome = Welcome {
        client_id: INTERNAL_CLIENT_ID,
        info: ServerInfo::new(version, spv, false),
    };

    if stx.send(ServerMessage::Welcome(welcome)).is_err() {
        return;
    }

    let client_info = ClientInfo {
        client_id: INTERNAL_CLIENT_ID,
        protocol: Protocol::Local,
    };

    while let Some(client_message) = crx.recv().await {
        match client_message {
            ClientMessage::ProtocolSwitchRequest(_) => {
                let _ = stx.send(ServerMessage::Ack(Ack { transaction_id: 0 }));
            }
            ClientMessage::AuthorizationRequest(_) => {
                let response = ServerMessage::Err(Err {
                    error_code: ErrorCode::AlreadyAuthorized,
                    metadata: "No authorization required".to_owned(),
                    transaction_id: 0,
                });
                let _ = stx.send(response);
            }
            ClientMessage::Get(req) => {
                let transaction_id = req.transaction_id;
                match api.get(req, client_info.clone()).await {
                    Ok(val) => {
                        let response = ServerMessage::State(State {
                            event: StateEvent::Value(val),
                            transaction_id,
                            trace: None,
                        });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::CGet(req) => {
                let transaction_id = req.transaction_id;
                match api.cget(req, client_info.clone()).await {
                    Ok(val) => {
                        let response = ServerMessage::CState(CState {
                            event: CStateEvent {
                                value: val.0,
                                version: val.1,
                            },
                            transaction_id,
                        });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::PGet(req) => {
                let transaction_id = req.transaction_id;
                let request_pattern = req.request_pattern.clone();
                match api.pget(req, client_info.clone()).await {
                    Ok(kvps) => {
                        let response = ServerMessage::PState(PState {
                            event: PStateEvent::KeyValuePairs(kvps),
                            request_pattern,
                            transaction_id,
                            trace: None,
                        });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Set(req) => {
                let transaction_id = req.transaction_id;
                match api.set(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::CSet(req) => {
                let transaction_id = req.transaction_id;
                match api.cset(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::SPubInit(req) => {
                let transaction_id = req.transaction_id;
                match api.spub_init(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::SPub(req) => {
                let transaction_id = req.transaction_id;
                match api.spub(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Publish(req) => {
                let transaction_id = req.transaction_id;
                match api.publish(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Subscribe(req) => {
                let transaction_id = req.transaction_id;
                match api.subscribe(req, client_info.clone()).await {
                    Ok((sub_rx, _)) => {
                        spawn_forward_sub_events_loop(sub_rx, transaction_id, stx.clone());
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::PSubscribe(req) => {
                let transaction_id = req.transaction_id;
                let request_pattern = req.request_pattern.clone();
                match api.psubscribe(req, client_info.clone()).await {
                    Ok((psub_rx, _)) => {
                        spawn_forward_psub_events_loop(
                            psub_rx,
                            transaction_id,
                            request_pattern,
                            stx.clone(),
                        );
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Unsubscribe(req) => {
                let transaction_id = req.transaction_id;
                match api.unsubscribe(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Delete(req) => {
                let transaction_id = req.transaction_id;
                match api.delete(req, client_info.clone()).await {
                    Ok(val) => {
                        let response = ServerMessage::State(State {
                            transaction_id,
                            event: StateEvent::Deleted(val),
                            trace: None,
                        });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::PDelete(req) => {
                let transaction_id = req.transaction_id;
                let request_pattern = req.request_pattern.clone();
                let quiet = req.quiet;
                match api.pdelete(req, client_info.clone()).await {
                    Ok(kvps) => {
                        let response = if quiet.unwrap_or(false) {
                            ServerMessage::PState(PState {
                                transaction_id,
                                request_pattern,
                                event: PStateEvent::Deleted(kvps),
                                trace: None,
                            })
                        } else {
                            ServerMessage::Ack(Ack { transaction_id })
                        };
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Ls(req) => {
                let transaction_id = req.transaction_id;
                match api.ls(req, client_info.clone()).await {
                    Ok(children) => {
                        let response = ServerMessage::LsState(LsState {
                            transaction_id,
                            children,
                            trace: None,
                        });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::PLs(req) => {
                let transaction_id = req.transaction_id;
                match api.pls(req, client_info.clone()).await {
                    Ok(children) => {
                        let response = ServerMessage::LsState(LsState {
                            transaction_id,
                            children,
                            trace: None,
                        });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::SubscribeLs(req) => {
                let transaction_id = req.transaction_id;
                match api.subscribe_ls(req, client_info.clone()).await {
                    Ok((lssub_rx, _)) => {
                        spawn_forward_lssub_events_loop(lssub_rx, transaction_id, stx.clone());
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::UnsubscribeLs(req) => {
                let transaction_id = req.transaction_id;
                match api.unsubscribe_ls(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Lock(req) => {
                let transaction_id = req.transaction_id;
                match api.lock(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::AcquireLock(req) => {
                let transaction_id = req.transaction_id;
                match api.acquire_lock(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::ReleaseLock(req) => {
                let transaction_id = req.transaction_id;
                match api.release_lock(req, client_info.clone()).await {
                    Ok(_) => {
                        let response = ServerMessage::Ack(Ack { transaction_id });
                        let _ = stx.send(response);
                    }
                    Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            } // ClientMessage::Transform(_) => todo!(),
        }
    }
}

async fn handle_error(
    tx: &mpsc::UnboundedSender<ServerMessage>,
    e: WorterbuchError,
    transaction_id: TransactionId,
) {
    let error_code = ErrorCode::from(&e);
    let err_msg = format!("{e}");
    let err = Err {
        error_code,
        transaction_id,
        metadata: json!(err_msg).to_string(),
    };
    let _ = tx.send(ServerMessage::Err(err));
}

fn spawn_forward_sub_events_loop(
    sub_rx: SubscriptionReceiver,
    transaction_id: TransactionId,
    stx: mpsc::UnboundedSender<ServerMessage>,
) {
    spawn(forward_sub_events(sub_rx, transaction_id, stx));
}

async fn forward_sub_events(
    mut sub_rx: SubscriptionReceiver,
    transaction_id: TransactionId,
    stx: mpsc::UnboundedSender<ServerMessage>,
) {
    while let Some((event, trace)) = sub_rx.recv().await {
        if stx
            .send(ServerMessage::State(State {
                transaction_id,
                event,
                trace,
            }))
            .is_err()
        {
            break;
        }
    }
}

fn spawn_forward_psub_events_loop(
    psub_rx: PSubscriptionReceiver,
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    stx: mpsc::UnboundedSender<ServerMessage>,
) {
    spawn(forward_psub_events(
        psub_rx,
        transaction_id,
        request_pattern,
        stx,
    ));
}

async fn forward_psub_events(
    mut psub_rx: PSubscriptionReceiver,
    transaction_id: TransactionId,
    request_pattern: RequestPattern,
    stx: mpsc::UnboundedSender<ServerMessage>,
) {
    while let Some((event, trace)) = psub_rx.recv().await {
        let request_pattern = request_pattern.clone();
        if stx
            .send(ServerMessage::PState(PState {
                transaction_id,
                request_pattern,
                event,
                trace,
            }))
            .is_err()
        {
            break;
        }
    }
}

fn spawn_forward_lssub_events_loop(
    lssub_rx: LsSubscriptionReceiver,
    transaction_id: TransactionId,
    stx: mpsc::UnboundedSender<ServerMessage>,
) {
    spawn(forward_lssub_events(lssub_rx, transaction_id, stx));
}

async fn forward_lssub_events(
    mut lssub_rx: LsSubscriptionReceiver,
    transaction_id: TransactionId,
    stx: mpsc::UnboundedSender<ServerMessage>,
) {
    while let Some((children, trace)) = lssub_rx.recv().await {
        if stx
            .send(ServerMessage::LsState(LsState {
                transaction_id,
                children,
                trace,
            }))
            .is_err()
        {
            break;
        }
    }
}
