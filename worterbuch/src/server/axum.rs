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
    server::common::{CloneableWbApi, init_server_socket},
    stats::VERSION,
};
use axum::{
    Json, Router,
    extract::{ConnectInfo, Path, Query, Request, State, WebSocketUpgrade, ws::WebSocket},
    http::{
        HeaderValue, Method,
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, SET_COOKIE},
    },
    middleware,
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{delete, get, post},
};
use axum_extra::{
    TypedHeader,
    extract::{
        CookieJar,
        cookie::{Cookie, SameSite},
    },
};
use axum_server::Handle;
use base64::{Engine, prelude::BASE64_STANDARD};
use flate2::{
    Compression,
    write::{GzDecoder, GzEncoder},
};
use futures::Stream;
use headers::{Authorization, ContentType, HeaderMapExt, authorization::Bearer};
use http_body_util::BodyExt;
use miette::IntoDiagnostic;
use serde_json::Value;
use std::{
    collections::HashMap,
    io::{self, ErrorKind, Write},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    thread,
    time::Duration,
};
use tokio::{
    select, spawn,
    sync::{mpsc, oneshot},
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use tracing::{debug, debug_span, error, info, instrument, warn};
use uuid::Uuid;
use websocket::serve;
use worterbuch_common::{
    AuthCheck, Key, KeyValuePairs, Privilege, Protocol, RegularKeySegment, ServerInfo, StateEvent,
    error::{AuthorizationError, WorterbuchError, WorterbuchResult},
};

async fn ws(
    ws: WebSocketUpgrade,
    State(ws_tx): State<mpsc::Sender<(WebSocket, SocketAddr)>>,
    ConnectInfo(remote): ConnectInfo<SocketAddr>,
) -> WorterbuchResult<Response> {
    info!("Client connected");

    let callback = move |socket| async move {
        ws_tx.send((socket, remote)).await.ok();
    };

    let res = ws.protocols(vec!["worterbuch"]).on_upgrade(callback);

    Ok(res)
}

async fn info(State(wb): State<CloneableWbApi>) -> WorterbuchResult<Json<ServerInfo>> {
    let supported_protocol_versions = SUPPORTED_PROTOCOL_VERSIONS.into();
    let config = wb.config().await?;
    let info = ServerInfo::new(
        VERSION.to_owned(),
        supported_protocol_versions,
        config.auth_token.is_some(),
    );

    Ok(Json(info))
}

#[instrument(skip(wb), err)]
async fn export(
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    request: Request,
) -> WorterbuchResult<Response> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern("#"))?;
    }

    let base64 = request.headers().get("accept") == Some(&HeaderValue::from_static("text/plain"));

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

    let mut response = compressed.into_response();

    let val = format!(r#"attachment; filename={file_name}.gz"#);
    if let Ok(header) = HeaderValue::from_str(&val) {
        response.headers_mut().insert(CONTENT_DISPOSITION, header);
    } else {
        warn!("Invalid Content-Disposition header value: {val}");
    }

    if base64 {
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    }

    Ok(response)
}

#[instrument(skip(wb), err)]
async fn import(
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    request: Request,
) -> WorterbuchResult<Response> {
    if let Some(privileges) = privileges {
        if let Err(e) = privileges.authorize(&Privilege::Write, AuthCheck::Pattern("#")) {
            return Err(WorterbuchError::from(e));
        }
    }

    let base64 = request.headers().typed_get::<ContentType>() == Some(ContentType::text());

    // this won't work if the body is an long running stream
    let data = request
        .into_body()
        .collect()
        .await
        .map_err(|e| WorterbuchError::Other(Box::new(e), "failed to read body".to_owned()))?
        .to_bytes();

    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        let res = decompress(&data, base64);
        tx.send(res).ok();
    });

    let json = match rx.await {
        Ok(Ok(it)) => it,
        Ok(Err(e)) => {
            return Err(WorterbuchError::Other(
                Box::new(e),
                "error while decompressing exported data".to_owned(),
            ));
        }
        Err(e) => {
            return Err(WorterbuchError::Other(
                Box::new(e),
                "error while decompressing exported data".to_owned(),
            ));
        }
    };
    let json = String::from_utf8_lossy(&json).to_string();

    wb.import(json).await.map(|_| ().into_response())
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

async fn get_value(
    Path(key): Path<Key>,
    content_type: Option<TypedHeader<ContentType>>,
    Query(params): Query<HashMap<String, String>>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Response> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&key))?;
    }
    let pointer = params.get("pointer");
    let raw = params.get("raw");

    let content_type = content_type.map(|h| h.0);

    match wb.get(key.clone()).await {
        Ok(value) => {
            if let Some(pointer) = pointer {
                let key = key + pointer;
                let extracted = value.pointer(pointer);
                if let Some(extracted) = extracted {
                    if raw.is_some() || content_type == Some(ContentType::text()) {
                        if let Value::String(str) = extracted {
                            return Ok(str.to_owned().into_response());
                        }
                    }
                    Ok(Json(extracted.to_owned()).into_response())
                } else {
                    Err(WorterbuchError::NoSuchValue(key))?
                }
            } else {
                if raw.is_some() || content_type == Some(ContentType::text()) {
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

async fn pget(
    Path(pattern): Path<Key>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Json<KeyValuePairs>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&pattern))?;
    }
    Ok(Json(wb.pget(pattern).await?))
}

async fn set(
    Path(key): Path<Key>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    Json(value): Json<Value>,
) -> WorterbuchResult<Json<&'static str>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Write, AuthCheck::Pattern(&key))?;
    }
    let client_id = Uuid::new_v4();
    wb.set(key, value, client_id).await?;
    Ok(Json("Ok"))
}

async fn publish(
    Path(key): Path<Key>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    Json(value): Json<Value>,
) -> WorterbuchResult<Json<&'static str>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Write, AuthCheck::Pattern(&key))?;
    }
    wb.publish(key, value).await?;
    Ok(Json("Ok"))
}

async fn delete_value(
    Path(key): Path<Key>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Json<Value>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Delete, AuthCheck::Pattern(&key))?;
    }
    let client_id = Uuid::new_v4();
    Ok(Json(wb.delete(key, client_id).await?))
}

async fn pdelete(
    Path(pattern): Path<Key>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Json<KeyValuePairs>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Delete, AuthCheck::Pattern(&pattern))?;
    }
    let client_id = Uuid::new_v4();
    Ok(Json(wb.pdelete(pattern, client_id).await?))
}

async fn ls(
    Path(parent): Path<Key>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Json<Vec<RegularKeySegment>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&format!("{parent}/?")))?;
    }
    Ok(Json(wb.ls(Some(parent)).await?))
}

async fn ls_root(
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Json<Vec<RegularKeySegment>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern("?"))?;
    }
    Ok(Json(wb.ls(None).await?))
}

async fn subscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
) -> WorterbuchResult<Sse<impl Stream<Item = Result<Event, axum::Error>>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&key))?;
    }
    let client_id = Uuid::new_v4();
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

    let (mut rx, _) = wb
        .subscribe(client_id, transaction_id, key, unique, live_only)
        .await?;
    let (sse_tx, sse_rx) = mpsc::channel(100);

    // TODO listen for shutdown requests
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(state) = recv {
                    match state {
                        StateEvent::Value(value) => {
                            match serde_json::to_string(&value) {
                                Ok(json) => {
                                    if let Err(e) = sse_tx.send(Event::default().json_data(json)).await {
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
                            if let Err(e) = sse_tx.send(Event::default().json_data("null")).await {
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
    Ok(Sse::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

async fn psubscribe(
    Path(key): Path<Key>,
    Query(params): Query<HashMap<String, String>>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
) -> WorterbuchResult<Sse<impl Stream<Item = Result<Event, axum::Error>>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&key))?;
    }
    let client_id = Uuid::new_v4();
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

    let (mut rx, _) = wb
        .psubscribe(client_id, transaction_id, key, unique, live_only)
        .await?;

    let (sse_tx, sse_rx) = mpsc::channel(100);

    // TODO listen for shutdown requests
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(pstate) = recv {
                    match serde_json::to_string(&pstate) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::default().json_data(json)).await {
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
    Ok(Sse::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

async fn subscribels_root(
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
) -> WorterbuchResult<Sse<impl Stream<Item = Result<Event, axum::Error>>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern("?"))?;
    }
    let client_id = Uuid::new_v4();
    connected(&wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let wb_unsub = wb.clone();

    let (mut rx, _) = wb.subscribe_ls(client_id, transaction_id, None).await?;

    let (sse_tx, sse_rx) = mpsc::channel(100);

    // TODO listen for shutdown requests
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(children) = recv {
                    match serde_json::to_string(&children) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::default().json_data(json)).await {
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
    Ok(Sse::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

async fn subscribels(
    Path(parent): Path<Key>,
    State(wb): State<CloneableWbApi>,
    privileges: Option<JwtClaims>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
) -> WorterbuchResult<Sse<impl Stream<Item = Result<Event, axum::Error>>>> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Read, AuthCheck::Pattern(&format!("{parent}/?")))?;
    }
    let client_id = Uuid::new_v4();
    connected(&wb, client_id, remote_addr).await?;
    let transaction_id = 1;
    let wb_unsub = wb.clone();

    let (mut rx, _) = wb
        .subscribe_ls(client_id, transaction_id, Some(parent))
        .await?;

    let (sse_tx, sse_rx) = mpsc::channel(100);

    // TODO listen for shutdown requests
    spawn(async move {
        'recv_loop: loop {
            select! {
                _ = sse_tx.closed() => break 'recv_loop,
                recv = rx.recv() => if let Some(children) = recv {
                    match serde_json::to_string(&children) {
                        Ok(json) => {
                            if let Err(e) = sse_tx.send(Event::default().json_data(json)).await {
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
    Ok(Sse::new(tokio_stream::wrappers::ReceiverStream::new(
        sse_rx,
    )))
}

#[cfg(feature = "jemalloc")]
#[instrument(ret)]
async fn get_heap_files_list(privileges: Option<JwtClaims>) -> WorterbuchResult<Response> {
    use worterbuch_common::profiling::list_heap_profile_files;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    let files = list_heap_profile_files().await.unwrap_or_else(Vec::new);

    Ok(Json(files).into_response())
}

#[cfg(feature = "jemalloc")]
#[instrument(ret)]
async fn get_live_heap(privileges: Option<JwtClaims>) -> WorterbuchResult<Response> {
    use worterbuch_common::profiling::get_live_heap_profile;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    let pprof = get_live_heap_profile().await?;

    let mut response = pprof.into_response();
    response.headers_mut().insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=heap.pb.gz"),
    );

    Ok(response)
}

#[cfg(feature = "jemalloc")]
#[instrument(ret)]
async fn get_live_flamegraph(privileges: Option<JwtClaims>) -> WorterbuchResult<Response> {
    use worterbuch_common::profiling::get_live_flamegraph;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    let pprof = get_live_flamegraph().await?;

    let mut response = pprof.into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("image/svg+xml"));

    Ok(response)
}

#[cfg(feature = "jemalloc")]
#[instrument(ret)]
async fn get_heap_file(
    Path(filename): Path<String>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Response> {
    use axum::http::StatusCode;
    use worterbuch_common::profiling::get_heap_profile_from_file;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    match get_heap_profile_from_file(&filename).await? {
        Some(pprof) => {
            let mut response = pprof.into_response();
            if let Ok(header) =
                HeaderValue::from_str(&format!("attachment; filename={filename}.gz"))
            {
                response.headers_mut().insert(CONTENT_DISPOSITION, header);
            } else {
                warn!("Invalid header value: attachment; filename={filename}.gz");
            }

            Ok(response)
        }
        None => Ok((StatusCode::NOT_FOUND, "not found").into_response()),
    }
}

#[cfg(feature = "jemalloc")]
#[instrument(ret, err)]
async fn get_flamegraph_file(
    Path(filename): Path<String>,
    privileges: Option<JwtClaims>,
) -> WorterbuchResult<Response> {
    use axum::http::StatusCode;
    use worterbuch_common::profiling::get_flamegraph_from_file;

    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::Profile, AuthCheck::Flag)?;
    }

    match get_flamegraph_from_file(&filename).await? {
        Some(svg) => {
            let mut response = svg.into_response();
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("image/svg+xml"));

            Ok(response)
        }
        None => Ok((StatusCode::NOT_FOUND, "not found").into_response()),
    }
}

#[instrument(ret, err)]
async fn login(
    privileges: Option<JwtClaims>,
    header_jwt: Option<TypedHeader<Authorization<Bearer>>>,
    jar: CookieJar,
) -> WorterbuchResult<CookieJar> {
    if let Some(privileges) = privileges {
        privileges.authorize(&Privilege::WebLogin, AuthCheck::Flag)?;
        privileges
    } else {
        return Err(WorterbuchError::AlreadyAuthorized);
    };

    if let Some(jwt) = header_jwt {
        Ok(jar.add(
            Cookie::build(("worterbuch_auth_jwt", jwt.token().to_owned()))
                .path("/api/v1/")
                .http_only(true)
                .same_site(SameSite::Strict)
                .secure(true),
        ))
    } else {
        Err(WorterbuchError::Unauthorized(
            AuthorizationError::MissingToken,
        ))
    }
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

    let mut app = Router::new();
    let mut api = Router::new();

    let mut wsserver = None;

    if ws_enabled {
        let (ws_stream_tx, ws_stream_rx) = mpsc::channel(1024);
        let wb = worterbuch.clone();
        wsserver = Some(subsys.start(SubsystemBuilder::new("wsserver", |s| {
            run_ws_server(s, ws_stream_rx, wb)
        })));
        info!("Serving websocket endpoint at {proto}://{public_addr}:{port}/ws");
        app = app.route("/ws", get(ws).with_state(ws_stream_tx));
    }

    let config = worterbuch.config().await?;
    let rest_api_version = 1;
    let rest_root = format!("/api/v{rest_api_version}");
    info!("Serving REST API at {rest_proto}://{public_addr}:{port}{rest_root}");
    api = api
        .route(&format!("{rest_root}/get/{{*key}}"), get(get_value))
        .route(&format!("{rest_root}/set/{{*key}}"), post(set))
        .route(&format!("{rest_root}/pget/{{*pattern}}"), get(pget))
        .route(&format!("{rest_root}/publish/{{*key}}"), post(publish))
        .route(
            &format!("{rest_root}/delete/{{*key}}"),
            delete(delete_value),
        )
        .route(
            &format!("{rest_root}/pdelete/{{*pattern}}"),
            delete(pdelete),
        )
        .route(&format!("{rest_root}/ls"), get(ls_root))
        .route(&format!("{rest_root}/ls/{{*parent}}"), get(ls))
        .route(&format!("{rest_root}/subscribe/{{*key}}"), get(subscribe))
        .route(
            &format!("{rest_root}/psubscribe/{{*pattern}}"),
            get(psubscribe),
        )
        .route(&format!("{rest_root}/subscribels"), get(subscribels_root))
        .route(
            &format!("{rest_root}/subscribels/{{*parent}}"),
            get(subscribels),
        )
        .route(&format!("{rest_root}/export"), get(export))
        .route(&format!("{rest_root}/import"), post(import))
        .route(&format!("{rest_root}/login"), post(login));

    #[cfg(feature = "jemalloc")]
    if std::env::var("MALLOC_CONF")
        .unwrap_or("".into())
        .contains("prof_active:true")
    {
        api = api
            .route(
                &format!("{rest_root}/debug/heap/list"),
                get(get_heap_files_list),
            )
            .route(&format!("{rest_root}/debug/heap/live"), get(get_live_heap))
            .route(
                &format!("{rest_root}/debug/heap/file/{{file}}"),
                get(get_heap_file),
            )
            .route(
                &format!("{rest_root}/debug/flamegraph/live"),
                get(get_live_flamegraph),
            )
            .route(
                &format!("{rest_root}/debug/flamegraph/file/{{file}}"),
                get(get_flamegraph_file),
            )
    }

    info!("Serving server info at {rest_proto}://{public_addr}:{port}/info");
    app = app.route("/info", get(info).with_state(worterbuch.clone()));

    // let metrics = TokioMetrics::new();

    // app = app.route("/metrics/tokio", metrics.exporter());

    if let Some(web_root_path) = &config.web_root_path {
        let web_root_path = PathBuf::from(web_root_path);
        info!(
            "Serving custom web app from {web_root_path:?} at {rest_proto}://{public_addr}:{port}/"
        );

        app = app.fallback_service(
            ServeDir::new(&web_root_path)
                .fallback(ServeFile::new(web_root_path.join("index.html"))),
        );
    }

    let handle = Handle::new();

    let listener = init_server_socket(bind_addr, port, config.clone())?;
    let mut server = axum_server::from_tcp(listener);
    server.http_builder().http2().enable_connect_protocol();

    let trace = TraceLayer::new_for_http();

    let mut cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers([SET_COOKIE, AUTHORIZATION])
        .allow_credentials(true);

    if let Some(origins) = &config.cors_allowed_origins {
        cors = cors.allow_origin(AllowOrigin::list(
            origins.iter().filter_map(|v| HeaderValue::from_str(v).ok()),
        ));
    }

    let mut serve = Box::pin(
        server.handle(handle.clone()).serve(
            api.layer(middleware::from_fn_with_state(config, auth::bearer_auth))
                .merge(app)
                .with_state(worterbuch)
                .layer(trace)
                .layer(cors)
                .into_make_service_with_connect_info::<SocketAddr>(),
        ),
    );

    select! {
        res = &mut serve => res.into_diagnostic()?,
        _ = subsys.on_shutdown_requested() => {
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
            serve.await.into_diagnostic()?;
        },
    }

    if let Some(wsserver) = wsserver {
        wsserver.initiate_shutdown();
        if let Err(e) = wsserver.join().await {
            error!("Error waiting for ws server to shut down: {e}");
        }
    }

    debug!("webserver subsystem completed.");

    Ok(())
}

async fn run_ws_server(
    subsys: SubsystemHandle,
    mut listener: mpsc::Receiver<(WebSocket, SocketAddr)>,
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
