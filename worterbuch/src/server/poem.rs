use crate::server::common::{process_incoming_message, CloneableWbApi, Protocol};
use anyhow::anyhow;
use futures::{
    sink::SinkExt,
    stream::{SplitSink, StreamExt},
};
use poem::{
    delete,
    endpoint::StaticFilesEndpoint,
    get, handler,
    http::StatusCode,
    listener::TcpListener,
    middleware::AddData,
    post,
    web::{
        sse::{Event, SSE},
        websocket::WebSocket,
        RemoteAddr,
    },
    web::{
        websocket::{Message, WebSocketStream},
        Data, Json, Path, Query,
    },
    Addr, EndpointExt, IntoResponse, Request, Result, Route,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    io,
    // env,
    net::{IpAddr, SocketAddr},
    time::{Duration, Instant},
};
use tokio::{
    select, spawn,
    sync::mpsc,
    time::{sleep, MissedTickBehavior},
};
use tokio_graceful_shutdown::SubsystemHandle;
use uuid::Uuid;
use worterbuch_common::{
    error::WorterbuchError, AuthToken, Key, KeyValuePairs, ProtocolVersion, RegularKeySegment,
    ServerMessage, StateEvent,
};

fn to_error_response<T>(e: WorterbuchError) -> Result<T> {
    match e {
        WorterbuchError::AuthenticationFailed => Err(poem::Error::new(e, StatusCode::UNAUTHORIZED)),
        WorterbuchError::IllegalMultiWildcard(_)
        | WorterbuchError::IllegalWildcard(_)
        | WorterbuchError::MultiWildcardAtIllegalPosition(_)
        | WorterbuchError::NoSuchValue(_)
        | WorterbuchError::HandshakeAlreadyDone
        | WorterbuchError::HandshakeRequired
        | WorterbuchError::MissingValue
        | WorterbuchError::ReadOnlyKey(_) => Err(poem::Error::new(e, StatusCode::BAD_REQUEST)),
        e => Err(poem::Error::new(e, StatusCode::INTERNAL_SERVER_ERROR)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestApiBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    auth_token: Option<AuthToken>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    value: Option<Value>,
}

#[handler]
async fn ws(
    ws: WebSocket,
    Data(data): Data<&(CloneableWbApi, ProtocolVersion, SubsystemHandle)>,
    req: &Request,
) -> Result<impl IntoResponse> {
    log::info!("Client connected");
    let worterbuch = data.0.clone();
    let proto_version = data.1.to_owned();
    let subsys = data.2.to_owned();
    if let Err(e) = worterbuch.authenticate(None).await {
        return to_error_response(e);
    }
    let remote = *req
        .remote_addr()
        .as_socket_addr()
        .expect("Client has no remote address.");
    Ok(ws
        .protocols(vec!["worterbuch"])
        .on_upgrade(move |socket| async move {
            if let Err(e) = serve(remote, worterbuch, socket, proto_version, subsys).await {
                log::error!("Error in WS connection: {e}");
            }
        }))
}

#[handler]
async fn get_value(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Value>> {
    let pointer = params.get("extract");
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    match wb.get(key).await {
        Ok((key, value)) => {
            if let Some(pointer) = pointer {
                let key = key + pointer;
                let extracted = value.pointer(pointer);
                if let Some(extracted) = extracted {
                    Ok(Json(extracted.to_owned()))
                } else {
                    to_error_response(WorterbuchError::NoSuchValue(key))
                }
            } else {
                Ok(Json(value))
            }
        }
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn pget(
    Path(pattern): Path<Key>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<KeyValuePairs>> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    match wb.pget(pattern).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn set(
    Path(key): Path<Key>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<&'static str>> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    if let Some(value) = body.value {
        match wb.set(key, value).await {
            Ok(()) => {}
            Err(e) => return to_error_response(e),
        }
        Ok(Json("Ok"))
    } else {
        to_error_response(WorterbuchError::MissingValue)
    }
}

#[handler]
async fn publish(
    Path(key): Path<Key>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<&'static str>> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    if let Some(value) = body.value {
        match wb.publish(key, value).await {
            Ok(()) => {}
            Err(e) => return to_error_response(e),
        }
        Ok(Json("Ok"))
    } else {
        to_error_response(WorterbuchError::MissingValue)
    }
}

#[handler]
async fn delete_value(
    Path(key): Path<Key>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Value>> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    match wb.delete(key).await {
        Ok(kvp) => Ok(Json(kvp.1)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn pdelete(
    Path(pattern): Path<Key>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<KeyValuePairs>> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    match wb.pdelete(pattern).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn ls(
    Path(key): Path<Key>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Vec<RegularKeySegment>>> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    match wb.ls(Some(key)).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn ls_root(
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Vec<RegularKeySegment>>> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    match wb.ls(None).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn subscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    let client_id = Uuid::new_v4();
    let remote_addr = if let Addr::SocketAddr(it) = addr {
        it
    } else {
        return to_error_response(WorterbuchError::IoError(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "only network socket connections are supported",
            ),
            "only network socket connections are supported".to_owned(),
        ));
    }
    .to_owned();
    if let Err(e) = wb.connected(client_id, remote_addr, Protocol::HTTP).await {
        log::error!("Error adding new client: {e}");
        return to_error_response(e);
    }
    let transaction_id = 1;
    let unique: bool = params.get("unique").map(|it| it == "true").unwrap_or(false);
    let live_only: bool = params
        .get("liveOnly")
        .map(|it| it == "true")
        .unwrap_or(false);
    let wb_unsub = wb.clone();
    match wb
        .subscribe(client_id, transaction_id, key, unique, live_only)
        .await
    {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: while let Some(pstate) = rx.recv().await {
                    let events: Vec<StateEvent> = pstate.into();
                    for e in events {
                        match serde_json::to_string(&e) {
                            Ok(json) => {
                                if let Err(e) = sse_tx.send(Event::message(json)).await {
                                    log::error!("Error forwarding state event: {e}");
                                    break 'recv_loop;
                                }
                            }
                            Err(e) => {
                                log::error!("Error serializiing state event: {e}");
                                break 'recv_loop;
                            }
                        }
                    }
                }
                if let Err(e) = wb_unsub.unsubscribe(client_id, transaction_id).await {
                    log::error!("Error stopping subscription: {e}");
                }
                if let Err(e) = wb_unsub.disconnected(client_id, remote_addr).await {
                    log::error!("Error disconnecting client: {e}");
                }
            });
            Ok(
                SSE::new(tokio_stream::wrappers::ReceiverStream::new(sse_rx))
                    .keep_alive(Duration::from_secs(5)),
            )
        }
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn psubscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    let client_id = Uuid::new_v4();
    let remote_addr = if let Addr::SocketAddr(it) = addr {
        it
    } else {
        return to_error_response(WorterbuchError::IoError(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "only network socket connections are supported",
            ),
            "only network socket connections are supported".to_owned(),
        ));
    }
    .to_owned();
    if let Err(e) = wb.connected(client_id, remote_addr, Protocol::HTTP).await {
        log::error!("Error adding new client: {e}");
        return to_error_response(e);
    }
    let transaction_id = 1;
    let unique: bool = params.get("unique").map(|it| it == "true").unwrap_or(false);
    let live_only: bool = params
        .get("liveOnly")
        .map(|it| it == "true")
        .unwrap_or(false);
    let wb_unsub = wb.clone();
    match wb
        .psubscribe(client_id, transaction_id, key, unique, live_only)
        .await
    {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: while let Some(pstate) = rx.recv().await {
                    match serde_json::to_string(&pstate) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::message(json)).await {
                                log::error!("Error forwarding state event: {e}");
                                break 'recv_loop;
                            }
                        }
                        Err(e) => {
                            log::error!("Error serializiing state event: {e}");
                            break 'recv_loop;
                        }
                    }
                }
                if let Err(e) = wb_unsub.unsubscribe(client_id, transaction_id).await {
                    log::error!("Error stopping subscription: {e}");
                }
                if let Err(e) = wb_unsub.disconnected(client_id, remote_addr).await {
                    log::error!("Error disconnecting client: {e}");
                }
            });
            Ok(
                SSE::new(tokio_stream::wrappers::ReceiverStream::new(sse_rx))
                    .keep_alive(Duration::from_secs(5)),
            )
        }
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn subscribels_root(
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    let client_id = Uuid::new_v4();
    let remote_addr = if let Addr::SocketAddr(it) = addr {
        it
    } else {
        return to_error_response(WorterbuchError::IoError(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "only network socket connections are supported",
            ),
            "only network socket connections are supported".to_owned(),
        ));
    }
    .to_owned();
    if let Err(e) = wb.connected(client_id, remote_addr, Protocol::HTTP).await {
        log::error!("Error adding new client: {e}");
        return to_error_response(e);
    }
    let transaction_id = 1;
    let wb_unsub = wb.clone();
    match wb.subscribe_ls(client_id, transaction_id, None).await {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: while let Some(pstate) = rx.recv().await {
                    match serde_json::to_string(&pstate) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::message(json)).await {
                                log::error!("Error forwarding state event: {e}");
                                break 'recv_loop;
                            }
                        }
                        Err(e) => {
                            log::error!("Error serializiing state event: {e}");
                            break 'recv_loop;
                        }
                    }
                }
                if let Err(e) = wb_unsub.unsubscribe_ls(client_id, transaction_id).await {
                    log::error!("Error stopping subscription: {e}");
                }
                if let Err(e) = wb_unsub.disconnected(client_id, remote_addr).await {
                    log::error!("Error disconnecting client: {e}");
                }
            });
            Ok(
                SSE::new(tokio_stream::wrappers::ReceiverStream::new(sse_rx))
                    .keep_alive(Duration::from_secs(5)),
            )
        }
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn subscribels(
    Path(parent): Path<Key>,
    Json(body): Json<RestApiBody>,
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let auth_token = body.auth_token;
    if let Err(e) = wb.authenticate(auth_token).await {
        return to_error_response(e);
    }
    let client_id = Uuid::new_v4();
    let remote_addr = if let Addr::SocketAddr(it) = addr {
        it
    } else {
        return to_error_response(WorterbuchError::IoError(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "only network socket connections are supported",
            ),
            "only network socket connections are supported".to_owned(),
        ));
    }
    .to_owned();
    if let Err(e) = wb.connected(client_id, remote_addr, Protocol::HTTP).await {
        log::error!("Error adding new client: {e}");
        return to_error_response(e);
    }
    let transaction_id = 1;
    let wb_unsub = wb.clone();
    match wb
        .subscribe_ls(client_id, transaction_id, Some(parent))
        .await
    {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: while let Some(pstate) = rx.recv().await {
                    match serde_json::to_string(&pstate) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::message(json)).await {
                                log::error!("Error forwarding state event: {e}");
                                break 'recv_loop;
                            }
                        }
                        Err(e) => {
                            log::error!("Error serializiing state event: {e}");
                            break 'recv_loop;
                        }
                    }
                }
                if let Err(e) = wb_unsub.unsubscribe_ls(client_id, transaction_id).await {
                    log::error!("Error stopping subscription: {e}");
                }
                if let Err(e) = wb_unsub.disconnected(client_id, remote_addr).await {
                    log::error!("Error disconnecting client: {e}");
                }
            });
            Ok(
                SSE::new(tokio_stream::wrappers::ReceiverStream::new(sse_rx))
                    .keep_alive(Duration::from_secs(5)),
            )
        }
        Err(e) => to_error_response(e),
    }
}

pub async fn start(
    worterbuch: CloneableWbApi,
    tls: bool,
    bind_addr: IpAddr,
    port: u16,
    public_addr: String,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let proto = if tls { "wss" } else { "ws" };

    let proto_versions = worterbuch
        .supported_protocol_versions()
        .await
        .unwrap_or(Vec::new());

    let addr = format!("{bind_addr}:{port}");

    let mut app = Route::new().nest(
        format!("/ws"),
        get(ws.data((
            worterbuch.clone(),
            proto_versions
                .iter()
                .last()
                .expect("cannot be none")
                .to_owned(),
            subsys.clone(),
        ))),
    );

    let config = worterbuch.config().await?;
    if let Some(web_root_path) = config.web_root_path {
        log::info!("Serving custom web app from {web_root_path} at http://{public_addr}:{port}/");

        app = app.nest(
            "/",
            StaticFilesEndpoint::new(web_root_path)
                .show_files_listing()
                .index_file("index.html"),
        );
    }

    for proto_ver in proto_versions {
        app = app
            .at("/api/v1/get/*", get(get_value))
            .at("/api/v1/set/*", post(set))
            .at("/api/v1/pget/*", get(pget))
            .at("/api/v1/publish/*", post(publish))
            .at("/api/v1/delete/*", delete(delete_value))
            .at("/api/v1/pdelete/*", delete(pdelete))
            .at("/api/v1/ls", get(ls_root))
            .at("/api/v1/ls/*", get(ls))
            .at("/api/v1/subscribe/*", get(subscribe))
            .at("/api/v1/psubscribe/*", get(psubscribe))
            .at("/api/v1/subscribels", get(subscribels_root))
            .at("/api/v1/subscribels/*", get(subscribels))
            .nest(
                format!("/ws/{proto_ver}"),
                get(ws.data((worterbuch.clone(), proto_ver.to_owned()))),
            );
        log::info!("Serving ws endpoint at {proto}://{public_addr}:{port}/ws/{proto_ver}");
    }

    poem::Server::new(TcpListener::bind(addr))
        .run_with_graceful_shutdown(
            app.with(AddData::new(worterbuch)),
            subsys.on_shutdown_requested(),
            Some(Duration::from_secs(1)),
        )
        .await?;

    Ok(())
}

async fn serve(
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
    proto_version: ProtocolVersion,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let client_id = Uuid::new_v4();

    log::info!("New client connected: {client_id} ({remote_addr})");

    worterbuch
        .connected(client_id, remote_addr, Protocol::WS)
        .await?;

    log::debug!("Receiving messages from client {client_id} ({remote_addr}) …",);

    if let Err(e) = serve_loop(
        client_id,
        remote_addr,
        worterbuch.clone(),
        websocket,
        proto_version,
        subsys,
    )
    .await
    {
        log::error!("Error in serve loop: {e}");
    }

    worterbuch.disconnected(client_id, remote_addr).await?;

    Ok(())
}

type WebSocketSender = SplitSink<WebSocketStream, poem::web::websocket::Message>;

async fn serve_loop(
    client_id: Uuid,
    remote_addr: SocketAddr,
    worterbuch: CloneableWbApi,
    websocket: WebSocketStream,
    proto_version: ProtocolVersion,
    subsys: SubsystemHandle,
) -> anyhow::Result<()> {
    let config = worterbuch.config().await?;
    let auth_token = config.auth_token;
    let handshake_required = auth_token.is_some();
    let send_timeout = config.send_timeout;
    let keepalive_timeout = config.keepalive_timeout;
    let mut keepalive_timer = tokio::time::interval(Duration::from_secs(1));
    let mut last_keepalive_tx = Instant::now();
    let mut last_keepalive_rx = Instant::now();
    let mut handshake_complete = false;
    keepalive_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let (mut ws_tx, mut ws_rx) = websocket.split();
    let (ws_send_tx, mut ws_send_rx) = mpsc::channel(config.channel_buffer_size);
    let (keepalive_tx_tx, mut keepalive_tx_rx) = mpsc::unbounded_channel();

    // websocket send loop
    let subsys_send = subsys.clone();
    spawn(async move {
        while let Some(msg) = ws_send_rx.recv().await {
            send_with_timeout(
                msg,
                &mut ws_tx,
                send_timeout,
                &keepalive_tx_tx,
                &subsys_send,
            )
            .await;
        }
    });

    loop {
        select! {
            recv = ws_rx.next() => if let Some(msg) = recv {
                match msg {
                    Ok(incoming_msg) => {
                        last_keepalive_rx = Instant::now();
                        if let Message::Text(text) = incoming_msg {
                            let (msg_processed, handshake) = process_incoming_message(
                                client_id,
                                &text,
                                &worterbuch,
                                &ws_send_tx,
                                &proto_version,handshake_required,handshake_complete
                            )
                            .await?;
                            handshake_complete |= msg_processed && handshake;
                            if !msg_processed {
                                break;
                            }
                        }
                    },
                    Err(e) => {
                        log::error!("Error in WebSocket connection: {e}");
                        break;
                    }
                }
            } else {
                log::info!("WS stream of client {client_id} ({remote_addr}) closed.");
                break;
            },
            recv = keepalive_tx_rx.recv() => match recv {
                Some(keepalive) => last_keepalive_tx = keepalive?,
                None => break,
            },
            _ = keepalive_timer.tick() => {
                // check how long ago the last websocket message was received
                check_client_keepalive(last_keepalive_rx, last_keepalive_tx, handshake_complete, client_id, keepalive_timeout)?;
                // send out websocket message if the last has been more than a second ago
                send_keepalive(last_keepalive_tx, &ws_send_tx, ).await?;
            }
        }
    }

    Ok(())
}

async fn send_keepalive(
    last_keepalive_tx: Instant,
    ws_send_tx: &mpsc::Sender<ServerMessage>,
) -> anyhow::Result<()> {
    if last_keepalive_tx.elapsed().as_secs() >= 1 {
        log::trace!("Sending keepalive");
        ws_send_tx.send(ServerMessage::Keepalive).await?;
    }
    Ok(())
}

fn check_client_keepalive(
    last_keepalive_rx: Instant,
    last_keepalive_tx: Instant,
    handshake_complete: bool,
    client_id: Uuid,
    keepalive_timeout: Duration,
) -> anyhow::Result<()> {
    let lag = last_keepalive_tx - last_keepalive_rx;

    if handshake_complete && lag >= Duration::from_secs(2) {
        log::warn!(
            "Client {} has been inactive for {} seconds …",
            client_id,
            lag.as_secs()
        );
    }

    if handshake_complete && lag >= keepalive_timeout {
        log::warn!(
            "Client {} has been inactive for too long. Disconnecting.",
            client_id
        );
        Err(anyhow!("Client has been inactive for too long"))
    } else {
        Ok(())
    }
}

async fn send_with_timeout(
    msg: ServerMessage,
    websocket: &mut WebSocketSender,
    send_timeout: Duration,
    result_handler: &mpsc::UnboundedSender<anyhow::Result<Instant>>,
    subsys: &SubsystemHandle,
) {
    let json = match serde_json::to_string(&msg) {
        Ok(it) => it,
        Err(e) => {
            handle_encode_error(e, subsys);
            return;
        }
    };

    let msg = Message::Text(json);

    select! {
        r = websocket.send(msg) => {
            if let Err(e) = r {
                result_handler.send(Err(e.into())).ok();
            } else {
                result_handler.send(Ok(Instant::now())).ok();
            }
        },
        _ = sleep(send_timeout) => {
            log::error!("Send timeout");
            result_handler.send(Err(anyhow!("Send timeout"))).ok();
        },
    }
}

fn handle_encode_error(e: serde_json::Error, subsys: &SubsystemHandle) {
    log::error!("Failed to encode a value to JSON: {e}");
    subsys.request_global_shutdown();
}
