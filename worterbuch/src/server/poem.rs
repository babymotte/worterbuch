/*
 *  Worterbuch server HTTP module
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

mod auth;
mod websocket;

use crate::{
    SUPPORTED_PROTOCOL_VERSIONS,
    auth::JwtClaims,
    server::{common::CloneableWbApi, poem::auth::BearerAuth},
    stats::VERSION,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use flate2::{
    Compression,
    write::{GzDecoder, GzEncoder},
};
use miette::IntoDiagnostic;
use poem::{
    Addr, Body, EndpointExt, IntoResponse, Request, Response, Route, delete,
    endpoint::StaticFilesEndpoint,
    get, handler,
    listener::TcpListener,
    middleware::{AddData, CookieJarManager, Cors, TokioMetrics},
    post,
    web::{
        Data, Json, Path, Query, RemoteAddr,
        cookie::Cookie,
        headers::{self, HeaderMapExt, authorization::Bearer},
        sse::{Event, SSE},
        websocket::{WebSocket, WebSocketStream},
    },
};
use serde_json::Value;
#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
use std::env;
use std::{
    collections::HashMap,
    io::{self, ErrorKind, Write},
    net::{IpAddr, SocketAddr},
    thread,
    time::Duration,
};
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot},
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tracing::{debug, debug_span, error, info, instrument};
use uuid::Uuid;
use websocket::serve;
use worterbuch_common::{
    AuthCheck, Key, KeyValuePairs, Privilege, Protocol, RegularKeySegment, ServerInfo, StateEvent,
    error::{AuthorizationError, WorterbuchError, WorterbuchResult},
};

#[handler]
fn ws(
    ws: WebSocket,
    Data(ws_tx): Data<&mpsc::Sender<(WebSocketStream, SocketAddr)>>,
    RemoteAddr(addr): &RemoteAddr,
) -> WorterbuchResult<impl IntoResponse> {
    info!("Client connected");
    let remote = to_socket_addr(addr)?;

    let ws_tx = ws_tx.clone();
    let callback = move |socket| async move {
        ws_tx.send((socket, remote)).await.ok();
    };

    let res = ws.protocols(vec!["worterbuch"]).on_upgrade(callback);

    Ok(res)
}

#[handler]
async fn info(Data(wb): Data<&CloneableWbApi>) -> WorterbuchResult<Json<ServerInfo>> {
    let supported_protocol_versions = SUPPORTED_PROTOCOL_VERSIONS.into();
    let config = wb.config().await?;
    let info = ServerInfo::new(
        VERSION.to_owned(),
        supported_protocol_versions,
        config.auth_token.is_some(),
    );

    Ok(Json(info))
}

#[handler]
#[instrument(skip(wb), err)]
async fn export(
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
    request: &Request,
) -> WorterbuchResult<Response> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern("#"))?;
    }

    let base64 = request.header("accept") == Some("text/plain");

    let config = wb.config().await?;
    let file_name = config
        .default_export_file_name
        .unwrap_or_else(|| "export".to_owned())
        + ".json";
    let span = debug_span!("export");
    let (exported, _, _) = wb.export(span).await?;
    let json = exported.to_string();

    let (tx, rx) = oneshot::channel();
    let compress_span = debug_span!("compress");
    thread::spawn(move || {
        let g = compress_span.enter();
        let res = compress(json.as_bytes(), base64);
        tx.send(res).ok();
        drop(g);
        drop(compress_span);
    });

    let compressed = match rx.await {
        Ok(Ok(it)) => it,
        Ok(Err(e)) => {
            return Err(WorterbuchError::Other(
                Box::new(e),
                "error while compressing exported data".to_owned(),
            ))?;
        }
        Err(e) => {
            return Err(WorterbuchError::Other(
                Box::new(e),
                "error while compressing exported data".to_owned(),
            ))?;
        }
    };

    let mut response = compressed
        .into_response()
        .with_header(
            "Content-Disposition",
            format!(r#"attachment; filename={file_name}.gz"#),
        )
        .into_response();

    if base64 {
        response = response.set_content_type("text/plain");
    }

    Ok(response)
}

#[handler]
async fn import(
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
    data: Body,
    request: &Request,
) -> Response {
    if let Some(privileges) = privileges {
        if let Err(e) = privileges.authorize(&Privilege::Write, AuthCheck::Pattern("#")) {
            return WorterbuchError::from(e).into_response();
        }
    }

    let base64 = request.content_type() == Some("text/plain");

    let data = match data.into_vec().await {
        Ok(it) => it,
        Err(e) => {
            return poem::Error::from(e).into_response();
        }
    };

    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        let res = decompress(&data, base64);
        tx.send(res).ok();
    });

    let json = match rx.await {
        Ok(Ok(it)) => it,
        Ok(Err(e)) => {
            return WorterbuchError::Other(
                Box::new(e),
                "error while decompressing exported data".to_owned(),
            )
            .into_response();
        }
        Err(e) => {
            return WorterbuchError::Other(
                Box::new(e),
                "error while decompressing exported data".to_owned(),
            )
            .into_response();
        }
    };
    let json = String::from_utf8_lossy(&json).to_string();

    match wb.import(json).await {
        Ok(_) => ().into_response(),
        Err(e) => e.into_response(),
    }
}

fn compress(data: &[u8], base64: bool) -> io::Result<Vec<u8>> {
    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(data)?;
    let bytes = e.finish()?;

    if base64 {
        Ok(BASE64_STANDARD.encode(bytes).into_bytes())
    } else {
        Ok(bytes)
    }
}

fn decompress(data: &[u8], base64: bool) -> io::Result<Vec<u8>> {
    let mut e = GzDecoder::new(Vec::new());

    if base64 {
        let mut decoded = vec![];
        BASE64_STANDARD
            .decode_vec(data, &mut decoded)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        e.write_all(&decoded)?;
    } else {
        e.write_all(data)?;
    }

    e.finish()
}

#[handler]
async fn get_value(
    req: &Request,
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Response> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&key))?;
    }
    let pointer = params.get("pointer");
    let raw = params.get("raw");
    let content_type = req.content_type().map(str::to_lowercase);
    match wb.get(key.clone()).await {
        Ok(value) => {
            if let Some(pointer) = pointer {
                let key = key + pointer;
                let extracted = value.pointer(pointer);
                if let Some(extracted) = extracted {
                    if raw.is_some() || content_type.as_deref() == Some("text/plain") {
                        if let Value::String(str) = extracted {
                            return Ok(str.to_owned().into_response());
                        }
                    }
                    Ok(Json(extracted.to_owned()).into_response())
                } else {
                    Err(WorterbuchError::NoSuchValue(key))?
                }
            } else {
                if raw.is_some() || content_type.as_deref() == Some("text/plain") {
                    if let Value::String(str) = value {
                        return Ok(str.into_response());
                    }
                }
                Ok(Json(value).into_response())
            }
        }
        Err(e) => Err(e)?,
    }
}

#[handler]
async fn pget(
    Path(pattern): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Json<KeyValuePairs>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&pattern))?;
    }
    Ok(Json(wb.pget(pattern).await?))
}

#[handler]
async fn set(
    Path(key): Path<Key>,
    Json(value): Json<Value>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Json<&'static str>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Write, AuthCheck::Pattern(&key))?;
    }
    let client_id = Uuid::new_v4();
    wb.set(key, value, client_id).await?;
    Ok(Json("Ok"))
}

#[handler]
async fn publish(
    Path(key): Path<Key>,
    Json(value): Json<Value>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Json<&'static str>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Write, AuthCheck::Pattern(&key))?;
    }
    wb.publish(key, value).await?;
    Ok(Json("Ok"))
}

#[handler]
async fn delete_value(
    Path(key): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Json<Value>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Delete, AuthCheck::Pattern(&key))?;
    }
    let client_id = Uuid::new_v4();
    Ok(Json(wb.delete(key, client_id).await?))
}

#[handler]
async fn pdelete(
    Path(pattern): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Json<KeyValuePairs>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Delete, AuthCheck::Pattern(&pattern))?;
    }
    let client_id = Uuid::new_v4();
    Ok(Json(wb.pdelete(pattern, client_id).await?))
}

#[handler]
async fn ls(
    Path(parent): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Json<Vec<RegularKeySegment>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&format!("{parent}/?")))?;
    }
    Ok(Json(wb.ls(Some(parent)).await?))
}

#[handler]
async fn ls_root(
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Json<Vec<RegularKeySegment>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern("?"))?;
    }
    Ok(Json(wb.ls(None).await?))
}

#[handler]
async fn subscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
    RemoteAddr(addr): &RemoteAddr,
) -> WorterbuchResult<SSE> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&key))?;
    }
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

    let (mut rx, _) = wb
        .subscribe(client_id, transaction_id, key, unique, live_only)
        .await?;
    let (sse_tx, sse_rx) = mpsc::channel(100);
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(state) = recv {
                    match state {
                        StateEvent::Value(value) => {
                            match serde_json::to_string(&value) {
                                Ok(json) => {
                                    if let Err(e) = sse_tx.send(Event::message(json)).await {
                                        error!("Error forwarding state event: {e}");
                                        break 'recv_loop;
                                    }
                                }
                                Err(e) => {
                                    error!("Error serializiing state event: {e}");
                                    break 'recv_loop;
                                }
                            }
                        },
                        StateEvent::Deleted(_) => {
                            if let Err(e) = sse_tx.send(Event::message("null")).await {
                                error!("Error forwarding state event: {e}");
                                break 'recv_loop;
                            }
                        },
                    }
                } else {
                    break 'recv_loop;
                }
            }
        }
        if let Err(e) = wb_unsub.unsubscribe(client_id, transaction_id).await {
            error!("Error stopping subscription: {e}");
        }
        if let Err(e) = wb_unsub.disconnected(client_id, Some(remote_addr)).await {
            error!("Error disconnecting client: {e}");
        }
    });
    Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

#[handler]
async fn psubscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
    RemoteAddr(addr): &RemoteAddr,
) -> WorterbuchResult<SSE> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&key))?;
    }
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

    let (mut rx, _) = wb
        .psubscribe(client_id, transaction_id, key, unique, live_only)
        .await?;

    let (sse_tx, sse_rx) = mpsc::channel(100);
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(pstate) = recv {
                    match serde_json::to_string(&pstate) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::message(json)).await {
                                error!("Error forwarding state event: {e}");
                                break 'recv_loop;
                            }
                        }
                        Err(e) => {
                            error!("Error serializiing state event: {e}");
                            break 'recv_loop;
                        }
                    }
                } else {
                    break 'recv_loop;
                }
            }
        }
        if let Err(e) = wb_unsub.unsubscribe(client_id, transaction_id).await {
            error!("Error stopping subscription: {e}");
        }
        if let Err(e) = wb_unsub.disconnected(client_id, Some(remote_addr)).await {
            error!("Error disconnecting client: {e}");
        }
    });
    Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

#[handler]
async fn subscribels_root(
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
    RemoteAddr(addr): &RemoteAddr,
) -> WorterbuchResult<SSE> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern("?"))?;
    }
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let wb_unsub = wb.clone();

    let (mut rx, _) = wb.subscribe_ls(client_id, transaction_id, None).await?;

    let (sse_tx, sse_rx) = mpsc::channel(100);
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(children) = recv {
                    match serde_json::to_string(&children) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::message(json)).await {
                                error!("Error forwarding state event: {e}");
                                break 'recv_loop;
                            }
                        }
                        Err(e) => {
                            error!("Error serializiing state event: {e}");
                            break 'recv_loop;
                        }
                    }
                } else {
                    break 'recv_loop;
                }
            }
        }
        if let Err(e) = wb_unsub.unsubscribe_ls(client_id, transaction_id).await {
            error!("Error stopping subscription: {e}");
        }
        if let Err(e) = wb_unsub.disconnected(client_id, Some(remote_addr)).await {
            error!("Error disconnecting client: {e}");
        }
    });
    Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

#[handler]
async fn subscribels(
    Path(parent): Path<Key>,
    Data(wb): Data<&CloneableWbApi>,
    Data(privileges): Data<&Option<JwtClaims>>,
    RemoteAddr(addr): &RemoteAddr,
) -> WorterbuchResult<SSE> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&format!("{parent}/?")))?;
    }
    let client_id = Uuid::new_v4();
    let remote_addr = to_socket_addr(addr)?;
    connected(wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let wb_unsub = wb.clone();

    let (mut rx, _) = wb
        .subscribe_ls(client_id, transaction_id, Some(parent))
        .await?;

    let (sse_tx, sse_rx) = mpsc::channel(100);
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(children) = recv {
                    match serde_json::to_string(&children) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::message(json)).await {
                                error!("Error forwarding state event: {e}");
                                break 'recv_loop;
                            }
                        }
                        Err(e) => {
                            error!("Error serializiing state event: {e}");
                            break 'recv_loop;
                        }
                    }
                } else {
                    break 'recv_loop;
                }
            }
        }
        if let Err(e) = wb_unsub.unsubscribe_ls(client_id, transaction_id).await {
            error!("Error stopping subscription: {e}");
        }
        if let Err(e) = wb_unsub.disconnected(client_id, Some(remote_addr)).await {
            error!("Error disconnecting client: {e}");
        }
    });
    Ok(SSE::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[instrument(ret)]
#[handler]
async fn get_heap_files_list(
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Response> {
    use crate::profiling::list_heap_profile_files;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    let files = list_heap_profile_files().await.unwrap_or_else(Vec::new);

    Ok(Json(files).into_response())
}

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[instrument(ret)]
#[handler]
async fn get_live_heap(Data(privileges): Data<&Option<JwtClaims>>) -> WorterbuchResult<Response> {
    use crate::profiling::get_live_heap_profile;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    let pprof = get_live_heap_profile().await?;

    let response = pprof.into_response();

    Ok(response
        .with_header("Content-Disposition", "attachment; filename=heap.pb.gz")
        .into_response())
}

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[instrument(ret)]
#[handler]
async fn get_live_flamegraph(
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Response> {
    use crate::profiling::get_live_flamegraph;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    let pprof = get_live_flamegraph().await?;

    let response = pprof.into_response();

    Ok(response
        .with_header("Content-Type", "image/svg+xml")
        .into_response())
}

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[instrument(ret)]
#[handler]
async fn get_heap_file(
    Path(filename): Path<String>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Response> {
    use poem::http::StatusCode;

    use crate::profiling::get_heap_profile_from_file;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    match get_heap_profile_from_file(&filename).await? {
        Some(pprof) => {
            let response = pprof.into_response();

            Ok(response
                .with_header(
                    "Content-Disposition",
                    format!("attachment; filename={filename}.gz"),
                )
                .into_response())
        }
        None => Ok((StatusCode::NOT_FOUND, "not found").into_response()),
    }
}

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[instrument(ret, err)]
#[handler]
async fn get_flamegraph_file(
    Path(filename): Path<String>,
    Data(privileges): Data<&Option<JwtClaims>>,
) -> WorterbuchResult<Response> {
    use poem::http::StatusCode;

    use crate::profiling::get_flamegraph_from_file;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    match get_flamegraph_from_file(&filename).await? {
        Some(svg) => {
            let response = svg.into_response();

            Ok(response
                .with_header("Content-Type", "image/svg+xml")
                .into_response())
        }
        None => Ok((StatusCode::NOT_FOUND, "not found").into_response()),
    }
}

#[instrument(ret, err)]
#[handler]
async fn login(req: &Request, Data(privileges): Data<&Option<JwtClaims>>) -> WorterbuchResult<()> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::WebLogin, AuthCheck::Flag)?;
        privileges
    } else {
        return Err(WorterbuchError::AlreadyAuthorized);
    };

    let header_jwt = req
        .headers()
        .typed_get::<headers::Authorization<Bearer>>()
        .map(|it| it.0.token().to_owned());

    if let Some(jwt) = header_jwt {
        let mut cookie = Cookie::named("worterbuch_auth_jwt");
        cookie.set_path("/api/v1/");
        cookie.set_value_str(jwt);
        cookie.set_http_only(true);
        // cookie.set_same_site(Some(SameSite::Strict));
        req.cookie().add(cookie);

        Ok(())
    } else {
        Err(WorterbuchError::Unauthorized(
            AuthorizationError::MissingToken,
        ))
    }
}

#[instrument(ret)]
#[handler]
async fn preflight(req: &Request) -> WorterbuchResult<()> {
    Ok(())
}

pub async fn start(
    worterbuch: CloneableWbApi,
    tls: bool,
    bind_addr: IpAddr,
    port: u16,
    public_addr: String,
    subsys: SubsystemHandle,
    ws_enabled: bool,
) -> miette::Result<()> {
    let proto = if tls { "wss" } else { "ws" };
    let rest_proto = if tls { "https" } else { "http" };

    let addr = format!("{bind_addr}:{port}");

    let mut app = Route::new();

    let mut wsserver = None;

    if ws_enabled {
        let (ws_stream_tx, ws_stream_rx) = mpsc::channel(1024);
        let wb = worterbuch.clone();
        wsserver = Some(subsys.start(SubsystemBuilder::new("wsserver", |s| {
            run_ws_server(s, ws_stream_rx, wb)
        })));
        info!("Serving websocket endpoint at {proto}://{public_addr}:{port}/ws");
        app = app.at("/ws", get(ws.with(AddData::new(ws_stream_tx))));
    }

    let config = worterbuch.config().await?;
    let rest_api_version = 1;
    let rest_root = format!("/api/v{rest_api_version}");
    info!("Serving REST API at {rest_proto}://{public_addr}:{port}{rest_root}");
    app = app
        .at(
            format!("{rest_root}/get/*"),
            get(get_value
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/set/*"),
            post(
                set.with(BearerAuth::new(config.clone()))
                    .with(AddData::new(worterbuch.clone())),
            )
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/pget/*"),
            get(pget
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/publish/*"),
            post(
                publish
                    .with(BearerAuth::new(config.clone()))
                    .with(AddData::new(worterbuch.clone())),
            )
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/delete/*"),
            delete(
                delete_value
                    .with(BearerAuth::new(config.clone()))
                    .with(AddData::new(worterbuch.clone())),
            )
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/pdelete/*"),
            delete(
                pdelete
                    .with(BearerAuth::new(config.clone()))
                    .with(AddData::new(worterbuch.clone())),
            )
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/ls"),
            get(ls_root
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/ls/*"),
            get(ls
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/subscribe/*"),
            get(subscribe
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/psubscribe/*"),
            get(psubscribe
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/subscribels"),
            get(subscribels_root
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/subscribels/*"),
            get(subscribels
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/export"),
            get(export
                .with(BearerAuth::new(config.clone()))
                .with(AddData::new(worterbuch.clone())))
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/import"),
            post(
                import
                    .with(BearerAuth::new(config.clone()))
                    .with(AddData::new(worterbuch.clone())),
            )
            .options(preflight.with(cors())),
        )
        .at(
            format!("{rest_root}/login"),
            post(login.with(BearerAuth::new(config.clone()))).options(preflight.with(cors())),
        );

    #[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
    if env::var("MALLOC_CONF")
        .unwrap_or("".into())
        .contains("prof_active:true")
    {
        app = app
            .at(
                format!("{rest_root}/debug/heap/list"),
                get(get_heap_files_list.with(BearerAuth::new(config.clone())))
                    .options(preflight.with(cors())),
            )
            .at(
                format!("{rest_root}/debug/heap/live"),
                get(get_live_heap.with(BearerAuth::new(config.clone())))
                    .options(preflight.with(cors())),
            )
            .at(
                format!("{rest_root}/debug/heap/file/:file"),
                get(get_heap_file.with(BearerAuth::new(config.clone())))
                    .options(preflight.with(cors())),
            )
            .at(
                format!("{rest_root}/debug/flamegraph/live"),
                get(get_live_flamegraph.with(BearerAuth::new(config.clone())))
                    .options(preflight.with(cors())),
            )
            .at(
                format!("{rest_root}/debug/flamegraph/file/:file"),
                get(get_flamegraph_file.with(BearerAuth::new(config.clone())))
                    .options(preflight.with(cors())),
            )
    }

    info!("Serving server info at {rest_proto}://{public_addr}:{port}/info");
    app = app.at("/info", get(info.with(AddData::new(worterbuch.clone()))));

    let metrics = TokioMetrics::new();

    app = app.at("/metrics/tokio", metrics.exporter());

    if let Some(web_root_path) = &config.web_root_path {
        info!(
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
            app.with(CookieJarManager::new()).with(metrics),
            subsys.on_shutdown_requested(),
            Some(Duration::from_secs(1)),
        )
        .await
        .into_diagnostic()?;

    if let Some(wsserver) = wsserver {
        wsserver.initiate_shutdown();
        if let Err(e) = wsserver.join().await {
            error!("Error waiting for ws server to shut down: {e}");
        }
    }

    debug!("webserver subsystem completed.");

    Ok(())
}

fn cors() -> Cors {
    Cors::new()
        .allow_credentials(true)
        .expose_header("Set-Cookie")
}

async fn run_ws_server(
    subsys: SubsystemHandle,
    mut listener: mpsc::Receiver<(WebSocketStream, SocketAddr)>,
    worterbuch: CloneableWbApi,
) -> WorterbuchResult<()> {
    let (conn_closed_tx, mut conn_closed_rx) = mpsc::channel(100);
    let mut waiting_for_free_connections = false;

    let mut clients = HashMap::new();

    loop {
        select! {
            recv = conn_closed_rx.recv() => if let Some(id) = recv {
                clients.remove(&id);
                while let Ok(id) = conn_closed_rx.try_recv() {
                    clients.remove(&id);
                }
                debug!("{} WS connection(s) open.", clients.len());
                waiting_for_free_connections = false;
            } else {
                break;
            },
            con = listener.recv(), if !waiting_for_free_connections => {
                debug!("Trying to accept new client connection.");
                if let Some((socket, remote_addr)) = con {
                    let id = Uuid::new_v4();
                    debug!("{} WS connection(s) open.",clients.len());
                    let worterbuch = worterbuch.clone();
                    let conn_closed_tx = conn_closed_tx.clone();

                    let client = subsys.start(SubsystemBuilder::new(format!("client-{id}"), move |s| async move {
                        select! {
                            s = serve(id, remote_addr, worterbuch, socket) => if let Err(e) = s {
                                error!("Connection to client {id} ({remote_addr:?}) closed with error: {e}");
                            },
                            _ = s.on_shutdown_requested() => (),
                        }
                        conn_closed_tx.send(id).await.ok();
                        Ok::<(),miette::Error>(())
                    }));
                    clients.insert(id, client);
                } else {
                    break;
                }
                debug!("Ready to accept new connections.");
            },
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    for (cid, subsys) in clients {
        subsys.initiate_shutdown();
        debug!("Waiting for connection to client {cid} to close …");
        if let Err(e) = subsys.join().await {
            error!("Error waiting for client {cid} to disconnect: {e}");
        }
    }
    debug!("All clients disconnected.");

    drop(listener);

    debug!("wsserver subsystem completed.");

    Ok(())
}

fn to_socket_addr(addr: &Addr) -> WorterbuchResult<SocketAddr> {
    if let Addr::SocketAddr(it) = addr {
        Ok(it.to_owned())
    } else {
        Err(WorterbuchError::IoError(
            io::Error::new(
                io::ErrorKind::Unsupported,
                "only network socket connections are supported",
            ),
            "only network socket connections are supported".to_owned(),
        ))?
    }
}

async fn connected(
    wb: &CloneableWbApi,
    client_id: Uuid,
    remote_addr: SocketAddr,
) -> WorterbuchResult<()> {
    if let Err(e) = wb
        .connected(client_id, Some(remote_addr), Protocol::HTTP)
        .await
    {
        error!("Error adding client {client_id} ({remote_addr}): {e}");
        Err(e)?
    } else {
        Ok(())
    }
}
