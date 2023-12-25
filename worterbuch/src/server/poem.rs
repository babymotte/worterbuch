mod auth;
mod websocket;

use crate::server::{
    common::{CloneableWbApi, Protocol},
    poem::auth::BearerAuth,
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
    error::WorterbuchError, Key, KeyValuePairs, ProtocolVersion, RegularKeySegment, StateEvent,
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
        | WorterbuchError::ReadOnlyKey(_) => Err(poem::Error::new(e, StatusCode::BAD_REQUEST)),
        e => Err(poem::Error::new(e, StatusCode::INTERNAL_SERVER_ERROR)),
    }
}

#[handler]
async fn ws(
    ws: WebSocket,
    Data(data): Data<&(CloneableWbApi, ProtocolVersion, SubsystemHandle)>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<impl IntoResponse> {
    log::info!("Client connected");
    let worterbuch = data.0.clone();
    let proto_version = data.1.to_owned();
    let subsys = data.2.to_owned();
    let remote = to_socket_addr(addr)?;
    Ok(ws
        .protocols(vec!["worterbuch"])
        .on_upgrade(move |socket| async move {
            if let Err(e) =
                websocket::serve(remote, worterbuch, socket, proto_version, subsys).await
            {
                log::error!("Error in WS connection: {e}");
            }
        }))
}

#[handler]
async fn get_value(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<Value>> {
    let pointer = params.get("extract");
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
    match wb.set(key, value).await {
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
    match wb.delete(key).await {
        Ok(kvp) => Ok(Json(kvp.1)),
        Err(e) => to_error_response(e),
    }
}

#[handler]
async fn pdelete(
    Path(pattern): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
) -> Result<Json<KeyValuePairs>> {
    match wb.pdelete(pattern).await {
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
    connected(&wb, client_id, remote_addr).await?;
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
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(&wb, client_id, remote_addr).await?;
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
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(&wb, client_id, remote_addr).await?;
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
    Data(wb): Data<&CloneableWbApi>,
    RemoteAddr(addr): &RemoteAddr,
) -> Result<SSE> {
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(&wb, client_id, remote_addr).await?;
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
    let rest_proto = if tls { "https" } else { "http" };

    let proto_versions = worterbuch
        .supported_protocol_versions()
        .await
        .unwrap_or(Vec::new());

    let addr = format!("{bind_addr}:{port}");

    let main_proto_version = proto_versions
        .iter()
        .last()
        .expect("cannot be none")
        .to_owned();
    log::info!("Serving ws endpoint for protocol version {main_proto_version} at {proto}://{public_addr}:{port}/ws");
    let mut app = Route::new().at(
        "/ws",
        get(ws.data((worterbuch.clone(), main_proto_version, subsys.clone()))),
    );

    for proto_ver in proto_versions {
        app = app.at(
            format!("/ws/{proto_ver}"),
            get(ws.data((worterbuch.clone(), proto_ver.to_owned()))),
        );
        log::info!("Serving ws endpoint at {proto}://{public_addr}:{port}/ws/{proto_ver}");
    }

    let rest_api_version = 1;
    let rest_root = format!("/api/v{rest_api_version}");
    log::info!("Serving REST API at {rest_proto}://{public_addr}:{port}{rest_root}");
    app = app
        .at(format!("{rest_root}/get/*"), get(get_value))
        .at(format!("{rest_root}/set/*"), post(set))
        .at(format!("{rest_root}/pget/*"), get(pget))
        .at(format!("{rest_root}/publish/*"), post(publish))
        .at(format!("{rest_root}/delete/*"), delete(delete_value))
        .at(format!("{rest_root}/pdelete/*"), delete(pdelete))
        .at(format!("{rest_root}/ls"), get(ls_root))
        .at(format!("{rest_root}/ls/*"), get(ls))
        .at(format!("{rest_root}/subscribe/*"), get(subscribe))
        .at(format!("{rest_root}/psubscribe/*"), get(psubscribe))
        .at(format!("{rest_root}/subscribels"), get(subscribels_root))
        .at(format!("{rest_root}/subscribels/*"), get(subscribels));

    let config = worterbuch.config().await?;
    if let Some(web_root_path) = config.web_root_path {
        log::info!(
            "Serving custom web app from {web_root_path} at {rest_proto}://{public_addr}:{port}/"
        );

        app = app.at(
            "/",
            StaticFilesEndpoint::new(web_root_path)
                .show_files_listing()
                .index_file("index.html"),
        );
    }

    poem::Server::new(TcpListener::bind(addr))
        .run_with_graceful_shutdown(
            app.with(AddData::new(worterbuch.clone()))
                .with(BearerAuth::new(worterbuch)),
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
        return to_error_response(WorterbuchError::IoError(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "only network socket connections are supported",
            ),
            "only network socket connections are supported".to_owned(),
        ));
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
