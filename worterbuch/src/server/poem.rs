mod auth;
mod websocket;

use crate::server::{common::CloneableWbApi, poem::auth::BearerAuth};
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
    web::{Data, Json, Path, Query},
    Addr, EndpointExt, IntoResponse, Result, Route,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    io,
    // env,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio::{select, spawn, sync::mpsc};
use tokio_graceful_shutdown::SubsystemHandle;
use uuid::Uuid;
use worterbuch_common::{
    error::WorterbuchError, Key, KeyValuePairs, Protocol, RegularKeySegment, ServerInfo, StateEvent,
};

fn to_error_response<T>(e: WorterbuchError) -> Result<T> {
    match e {
        WorterbuchError::AuthenticationFailed => Err(poem::Error::new(e, StatusCode::UNAUTHORIZED)),
        WorterbuchError::IllegalMultiWildcard(_)
        | WorterbuchError::IllegalWildcard(_)
        | WorterbuchError::MultiWildcardAtIllegalPosition(_)
        | WorterbuchError::NoSuchValue(_)
        | WorterbuchError::AlreadyAuthenticated
        | WorterbuchError::AuthenticationRequired(_)
        | WorterbuchError::ReadOnlyKey(_) => Err(poem::Error::new(e, StatusCode::BAD_REQUEST)),
        e => Err(poem::Error::new(e, StatusCode::INTERNAL_SERVER_ERROR)),
    }
}

#[handler]
fn ws(
    ws: WebSocket,
    Data(wb): Data<&CloneableWbApi>,
    Data(subsys): Data<&SubsystemHandle>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<impl IntoResponse> {
    log::info!("Client connected");
    let worterbuch = wb.to_owned();
    let subsys = subsys.to_owned();
    let remote = to_socket_addr(addr)?;
    Ok(ws
        .protocols(vec!["worterbuch"])
        .on_upgrade(move |socket| async move {
            if let Err(e) = websocket::serve(remote, worterbuch, socket, subsys).await {
                log::error!("Error in WS connection: {e}");
            }
        }))
}

#[handler]
async fn info(Data(wb): Data<&CloneableWbApi>) -> Result<Json<ServerInfo>> {
    let proto = match wb.supported_protocol_version().await {
        Ok(it) => it,
        Err(e) => return to_error_response(e),
    };
    let config = match wb.config().await {
        Ok(it) => it,
        Err(e) => return to_error_response(e),
    };
    let info = ServerInfo {
        authentication_required: config.auth_token.is_some(),
        protocol_version: proto,
    };

    Ok(Json(info))
}

#[handler]
async fn get_value(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Value>> {
    let pointer = params.get("pointer");
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
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<KeyValuePairs>> {
    match wb.pget(pattern).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn set(
    Path(key): Path<Key>,
    Json(value): Json<Value>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<&'static str>> {
    let client_id = Uuid::new_v4();
    match wb.set(key, value, client_id.to_string()).await {
        Ok(()) => Ok(Json("Ok")),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn publish(
    Path(key): Path<Key>,
    Json(value): Json<Value>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<&'static str>> {
    match wb.publish(key, value).await {
        Ok(()) => Ok(Json("Ok")),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn delete_value(
    Path(key): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Value>> {
    let client_id = Uuid::new_v4();
    match wb.delete(key, client_id.to_string()).await {
        Ok(kvp) => Ok(Json(kvp.1)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn pdelete(
    Path(pattern): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<KeyValuePairs>> {
    let client_id = Uuid::new_v4();
    match wb.pdelete(pattern, client_id.to_string()).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn ls(
    Path(key): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Vec<RegularKeySegment>>> {
    match wb.ls(Some(key)).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn ls_root(Data(wb): Data<&CloneableWbApi>) -> Result<Json<Vec<RegularKeySegment>>> {
    match wb.ls(None).await {
        Ok(kvps) => Ok(Json(kvps)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn subscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let unique: bool = params
        .get("unique")
        .map(|it| it.to_lowercase() != "false")
        .unwrap_or(false);
    let live_only: bool = params
        .get("liveOnly")
        .map(|it| it.to_lowercase() != "false")
        .unwrap_or(false);
    let raw: bool = params
        .get("raw")
        .map(|it| it.to_lowercase() != "false")
        .unwrap_or(false);
    let wb_unsub = wb.clone();
    match wb
        .subscribe(client_id, transaction_id, key, unique, live_only)
        .await
    {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: loop {
                    select! {
                        _ = sse_tx.closed() => break 'recv_loop,
                        recv = rx.recv() => if let Some(pstate) = recv {
                            let events: Vec<StateEvent> = pstate.into();
                            for e in events {
                                if raw {
                                    match &e {
                                        StateEvent::KeyValue(kv) => {
                                            match serde_json::to_string(&kv.value) {
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
                                        },
                                        StateEvent::Deleted(_) => {
                                            if let Err(e) = sse_tx.send(Event::message("null")).await {
                                                log::error!("Error forwarding state event: {e}");
                                                break 'recv_loop;
                                            }
                                        }
                                    }
                                } else {
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
                        } else {
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
            Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
                sse_rx,
            )))
        }
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn psubscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let unique: bool = params
        .get("unique")
        .map(|it| it.to_lowercase() != "false")
        .unwrap_or(false);
    let live_only: bool = params
        .get("liveOnly")
        .map(|it| it.to_lowercase() != "false")
        .unwrap_or(false);
    let wb_unsub = wb.clone();
    match wb
        .psubscribe(client_id, transaction_id, key, unique, live_only)
        .await
    {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: loop {
                    select! {
                        _ = sse_tx.closed() => break 'recv_loop,
                        recv = rx.recv() => if let Some(pstate) = recv {
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
                        } else {
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
            Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
                sse_rx,
            )))
        }
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn subscribels_root(
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let wb_unsub = wb.clone();
    match wb.subscribe_ls(client_id, transaction_id, None).await {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: loop {
                    select! {
                        _ = sse_tx.closed() => break 'recv_loop,
                        recv = rx.recv() => if let Some(children) = recv {
                            match serde_json::to_string(&children) {
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
                        } else {
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
            Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
                sse_rx,
            )))
        }
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn subscribels(
    Path(parent): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let wb_unsub = wb.clone();
    match wb
        .subscribe_ls(client_id, transaction_id, Some(parent))
        .await
    {
        Ok((mut rx, _)) => {
            let (sse_tx, sse_rx) = mpsc::channel(100);
            spawn(async move {
                'recv_loop: loop {
                    select! {
                        _ = sse_tx.closed() => break 'recv_loop,
                        recv = rx.recv() => if let Some(children) = recv {
                            match serde_json::to_string(&children) {
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
                        } else {
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
            Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
                sse_rx,
            )))
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
    let rest_proto = if tls { "https" } else { "http" };

    let addr = format!("{bind_addr}:{port}");

    log::info!("Serving websocket endpoint at {proto}://{public_addr}:{port}/ws");
    let mut app = Route::new();

    app = app.at(
        "/ws",
        get(ws
            .with(AddData::new(worterbuch.clone()))
            .with(AddData::new(subsys.clone()))),
    );

    let rest_api_version = 1;
    let rest_root = format!("/api/v{rest_api_version}");
    log::info!("Serving REST API at {rest_proto}://{public_addr}:{port}{rest_root}");
    app = app
        .at(
            format!("{rest_root}/get/*"),
            get(get_value
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        )
        .at(
            format!("{rest_root}/set/*"),
            post(
                set.with(BearerAuth::new(worterbuch.clone()))
                    .with(AddData::new(worterbuch.clone())),
            ),
        )
        .at(
            format!("{rest_root}/pget/*"),
            get(pget
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        )
        .at(
            format!("{rest_root}/publish/*"),
            post(
                publish
                    .with(BearerAuth::new(worterbuch.clone()))
                    .with(AddData::new(worterbuch.clone())),
            ),
        )
        .at(
            format!("{rest_root}/delete/*"),
            delete(
                delete_value
                    .with(BearerAuth::new(worterbuch.clone()))
                    .with(AddData::new(worterbuch.clone())),
            ),
        )
        .at(
            format!("{rest_root}/pdelete/*"),
            delete(
                pdelete
                    .with(BearerAuth::new(worterbuch.clone()))
                    .with(AddData::new(worterbuch.clone())),
            ),
        )
        .at(
            format!("{rest_root}/ls"),
            get(ls_root
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        )
        .at(
            format!("{rest_root}/ls/*"),
            get(ls
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        )
        .at(
            format!("{rest_root}/subscribe/*"),
            get(subscribe
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        )
        .at(
            format!("{rest_root}/psubscribe/*"),
            get(psubscribe
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        )
        .at(
            format!("{rest_root}/subscribels"),
            get(subscribels_root
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        )
        .at(
            format!("{rest_root}/subscribels/*"),
            get(subscribels
                .with(BearerAuth::new(worterbuch.clone()))
                .with(AddData::new(worterbuch.clone()))),
        );

    log::info!("Serving server info at {rest_proto}://{public_addr}:{port}/info");
    app = app.at("/info", get(info.with(AddData::new(worterbuch.clone()))));

    let config = worterbuch.config().await?;
    if let Some(web_root_path) = config.web_root_path {
        log::info!(
            "Serving custom web app from {web_root_path} at {rest_proto}://{public_addr}:{port}/"
        );

        app = app.nest(
            "/",
            StaticFilesEndpoint::new(web_root_path)
                .index_file("index.html")
                .fallback_to_index()
                .redirect_to_slash_directory(),
        );
    }

    poem::Server::new(TcpListener::bind(addr))
        .run_with_graceful_shutdown(
            app,
            subsys.on_shutdown_requested(),
            Some(Duration::from_secs(1)),
        )
        .await?;

    Ok(())
}

fn to_socket_addr(addr: &Addr) -> Result<SocketAddr> {
    if let Addr::SocketAddr(it) = addr {
        Ok(it.to_owned())
    } else {
        to_error_response(WorterbuchError::IoError(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "only network socket connections are supported",
            ),
            "only network socket connections are supported".to_owned(),
        ))
    }
}

async fn connected(wb: &CloneableWbApi, client_id: Uuid, remote_addr: SocketAddr) -> Result<()> {
    if let Err(e) = wb.connected(client_id, remote_addr, Protocol::HTTP).await {
        log::error!("Error adding client {client_id} ({remote_addr}): {e}");
        to_error_response(e)
    } else {
        Ok(())
    }
}
