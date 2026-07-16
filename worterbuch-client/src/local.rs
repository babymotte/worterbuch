use serde_json::json;
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
};
use worterbuch_common::{
    INTERNAL_CLIENT_ID, LsSubscriptionReceiver, PSubscriptionReceiver, SubscriptionReceiver, WbApi,
    error::{ConnectionResult, WorterbuchError},
    protocol::v1::{
        Ack, CSet, CState, CStateEvent, ClientMessage, Delete, Err, ErrorCode, Get, Lock, Ls,
        LsState, PDelete, PGet, PLs, PState, PStateEvent, PSubscribe, Publish, RequestPattern,
        SPub, SPubInit, ServerInfo, ServerMessage, Set, State, StateEvent, Subscribe, SubscribeLs,
        TransactionId, Unsubscribe, UnsubscribeLs, Welcome,
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
        self.closed.await.ok();
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
        client_id: INTERNAL_CLIENT_ID.to_string(),
        info: ServerInfo::new(version, spv.into(), false),
    };

    if stx.send(ServerMessage::Welcome(welcome)).is_err() {
        return;
    }

    while let Some(client_message) = crx.recv().await {
        match client_message {
            ClientMessage::ProtocolSwitchRequest(_) => {
                stx.send(ServerMessage::Ack(Ack { transaction_id: 0 })).ok();
            }
            ClientMessage::AuthorizationRequest(_) => {
                stx.send(ServerMessage::Err(Err {
                    error_code: ErrorCode::AlreadyAuthorized,
                    metadata: "No authorization required".to_owned(),
                    transaction_id: 0,
                }))
                .ok();
            }
            ClientMessage::Get(Get {
                transaction_id,
                key,
            }) => match api.get(key).await {
                Ok(val) => {
                    stx.send(ServerMessage::State(State {
                        event: StateEvent::Value(val),
                        transaction_id,
                        trace: None,
                    }))
                    .ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::CGet(Get {
                transaction_id,
                key,
            }) => match api.cget(key).await {
                Ok(val) => {
                    stx.send(ServerMessage::CState(CState {
                        event: CStateEvent {
                            value: val.0,
                            version: val.1,
                        },
                        transaction_id,
                    }))
                    .ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::PGet(PGet {
                transaction_id,
                request_pattern,
            }) => match api.pget(request_pattern.clone()).await {
                Ok(kvps) => {
                    stx.send(ServerMessage::PState(PState {
                        event: PStateEvent::KeyValuePairs(kvps),
                        request_pattern,
                        transaction_id,
                        trace: None,
                    }))
                    .ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::Set(Set {
                transaction_id,
                key,
                value,
            }) => match api
                .set(transaction_id, key, value, INTERNAL_CLIENT_ID)
                .await
            {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::CSet(CSet {
                transaction_id,
                key,
                value,
                version,
            }) => match api
                .cset(transaction_id, key, value, version, INTERNAL_CLIENT_ID)
                .await
            {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::SPubInit(SPubInit {
                transaction_id,
                key,
            }) => match api.spub_init(transaction_id, key, INTERNAL_CLIENT_ID).await {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::SPub(SPub {
                transaction_id,
                value,
            }) => match api.spub(transaction_id, value, INTERNAL_CLIENT_ID).await {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::Publish(Publish {
                transaction_id,
                key,
                value,
            }) => match api
                .publish(transaction_id, key, value, INTERNAL_CLIENT_ID)
                .await
            {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::Subscribe(Subscribe {
                transaction_id,
                key,
                unique,
                live_only,
                send_traces,
            }) => match api
                .subscribe(
                    INTERNAL_CLIENT_ID,
                    transaction_id,
                    key,
                    unique.unwrap_or(false),
                    live_only.unwrap_or(false),
                    send_traces.unwrap_or(false),
                )
                .await
            {
                Ok((sub_rx, _)) => {
                    spawn_forward_sub_events_loop(sub_rx, transaction_id, stx.clone());
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::PSubscribe(PSubscribe {
                transaction_id,
                request_pattern,
                unique,
                live_only,
                send_traces,
                aggregate_events: _,
            }) => match api
                .psubscribe(
                    INTERNAL_CLIENT_ID,
                    transaction_id,
                    request_pattern.clone(),
                    unique.unwrap_or(false),
                    live_only.unwrap_or(false),
                    send_traces.unwrap_or(false),
                )
                .await
            {
                Ok((psub_rx, _)) => {
                    spawn_forward_psub_events_loop(
                        psub_rx,
                        transaction_id,
                        request_pattern,
                        stx.clone(),
                    );
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::Unsubscribe(Unsubscribe { transaction_id }) => {
                match api.unsubscribe(INTERNAL_CLIENT_ID, transaction_id).await {
                    Ok(_) => {
                        stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                    }
                    Result::Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Delete(Delete {
                transaction_id,
                key,
            }) => match api.delete(transaction_id, key, INTERNAL_CLIENT_ID).await {
                Ok(val) => {
                    stx.send(ServerMessage::State(State {
                        transaction_id,
                        event: StateEvent::Deleted(val),
                        trace: None,
                    }))
                    .ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::PDelete(PDelete {
                transaction_id,
                request_pattern,
                quiet,
            }) => match api
                .pdelete(
                    transaction_id,
                    request_pattern.clone(),
                    quiet,
                    INTERNAL_CLIENT_ID,
                )
                .await
            {
                Ok(kvps) => {
                    if quiet.unwrap_or(false) {
                        stx.send(ServerMessage::PState(PState {
                            transaction_id,
                            request_pattern,
                            event: PStateEvent::Deleted(kvps),
                            trace: None,
                        }))
                        .ok();
                    } else {
                        stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                    }
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::Ls(Ls {
                transaction_id,
                parent,
            }) => match api.ls(parent).await {
                Ok(children) => {
                    stx.send(ServerMessage::LsState(LsState {
                        transaction_id,
                        children,
                        trace: None,
                    }))
                    .ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::PLs(PLs {
                transaction_id,
                parent_pattern,
            }) => match api.pls(parent_pattern).await {
                Ok(children) => {
                    stx.send(ServerMessage::LsState(LsState {
                        transaction_id,
                        children,
                        trace: None,
                    }))
                    .ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::SubscribeLs(SubscribeLs {
                transaction_id,
                parent,
                send_traces,
            }) => match api
                .subscribe_ls(
                    INTERNAL_CLIENT_ID,
                    transaction_id,
                    parent,
                    send_traces.unwrap_or(false),
                )
                .await
            {
                Ok((lssub_rx, _)) => {
                    spawn_forward_lssub_events_loop(lssub_rx, transaction_id, stx.clone());
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::UnsubscribeLs(UnsubscribeLs { transaction_id }) => {
                match api.unsubscribe_ls(INTERNAL_CLIENT_ID, transaction_id).await {
                    Ok(_) => {
                        stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                    }
                    Result::Err(e) => handle_error(&stx, e, transaction_id).await,
                }
            }
            ClientMessage::Lock(Lock {
                transaction_id,
                key,
            }) => match api.lock(transaction_id, key, INTERNAL_CLIENT_ID).await {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::AcquireLock(Lock {
                transaction_id,
                key,
            }) => match api
                .acquire_lock(transaction_id, key, INTERNAL_CLIENT_ID)
                .await
            {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            ClientMessage::ReleaseLock(Lock {
                transaction_id,
                key,
            }) => match api
                .release_lock(transaction_id, key, INTERNAL_CLIENT_ID)
                .await
            {
                Ok(_) => {
                    stx.send(ServerMessage::Ack(Ack { transaction_id })).ok();
                }
                Result::Err(e) => handle_error(&stx, e, transaction_id).await,
            },
            // ClientMessage::Transform(_) => todo!(),
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
    tx.send(ServerMessage::Err(err)).ok();
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
