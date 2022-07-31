#[cfg(feature = "graphql")]
mod graphql;

#[cfg(feature = "ws")]
use super::common::process_incoming_message;
#[cfg(feature = "graphql")]
use crate::server::web::graphql::Context;
use crate::{config::Config, worterbuch::Worterbuch};
#[cfg(feature = "ws")]
use anyhow::Result;
#[cfg(feature = "ws")]
use futures::{sink::SinkExt, stream::StreamExt};
#[cfg(feature = "graphql")]
use juniper_graphql_ws::ConnectionConfig;
#[cfg(feature = "graphql")]
use juniper_warp::subscriptions::serve_graphql_ws;
#[cfg(feature = "ws")]
use std::net::SocketAddr;
use std::{env, sync::Arc};
use tokio::sync::RwLock;
#[cfg(feature = "ws")]
use tokio::{spawn, sync::mpsc};
use warp::Filter;
#[cfg(feature = "ws")]
use warp::{addr::remote, ws::Message, ws::Ws};

pub(crate) async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) {
    log::info!("Starting Web Server …");

    #[cfg(feature = "ws")]
    let wb_ws = worterbuch.clone();
    #[cfg(feature = "graphql")]
    let wb_gql = worterbuch.clone();

    #[cfg(feature = "ws")]
    let ws = warp::ws().and(warp::path("ws")).and(remote()).map(
        move |ws: Ws, remote: Option<SocketAddr>| {
            let worterbuch = wb_ws.clone();
            ws.on_upgrade(move |websocket| async move {
                if let Err(e) = serve_ws(websocket, worterbuch.clone(), remote.clone()).await {
                    log::error!("Error in WS connection: {e}");
                }
            })
        },
    );

    #[cfg(feature = "graphql")]
    let graphiql = warp::get()
        .and(warp::path("graphiql"))
        .and(juniper_warp::graphiql_filter(
            "/graphql",
            Some("/graphql-ws"),
        ));

    #[cfg(feature = "graphql")]
    let (graphql, ws_context, ws_schema) = {
        let context = Context { database: wb_gql };
        let ws_schema = Arc::new(graphql::schema());
        let ws_context = context.clone();
        let graphql_schema = graphql::schema();
        let state = warp::any().map(move || context.clone());
        let graphql_filter = juniper_warp::make_graphql_filter(graphql_schema, state.boxed());
        (
            warp::path("graphql").and(graphql_filter),
            ws_context,
            ws_schema,
        )
    };

    #[cfg(feature = "graphql")]
    let graphql_ws = warp::path("graphql-ws")
        .and(warp::ws())
        .map(move |ws: warp::ws::Ws| {
            let graphql_ws_impl = ConnectionConfig::new(ws_context.clone());
            let ws_schema = ws_schema.clone();
            ws.on_upgrade(move |websocket| async move {
                if let Err(e) = serve_graphql_ws(websocket, ws_schema, graphql_ws_impl).await {
                    println!("Websocket error: {}", e);
                }
            })
        });

    #[cfg(feature = "explorer")]
    let explorer = warp::fs::dir(
        env::var("WORTERBUCH_EXPLORER_WEBROOT_PATH")
            .unwrap_or("../worterbuch-explorer/build".to_owned()),
    );

    #[cfg(feature = "ws")]
    let ws_route = ws;
    #[cfg(feature = "graphql")]
    let gql_route = graphql.or(graphql_ws).or(graphiql);
    #[cfg(feature = "explorer")]
    let expl_route = explorer;

    #[cfg(all(feature = "ws", not(feature = "graphql"), not(feature = "explorer")))]
    let routes = ws_route;
    #[cfg(all(feature = "graphql", not(feature = "ws"), not(feature = "explorer")))]
    let routes = gql_route;
    #[cfg(all(feature = "explorer", not(feature = "ws"), not(feature = "graphql")))]
    let routes = expl_route;

    #[cfg(all(feature = "ws", feature = "graphql", not(feature = "explorer")))]
    let routes = ws_route.or(gql_route);
    #[cfg(all(feature = "graphql", feature = "explorer", not(feature = "ws")))]
    let routes = gql_route.or(expl_route);
    #[cfg(all(feature = "explorer", feature = "ws", not(feature = "graphql")))]
    let routes = expl_route.or(ws_route);

    #[cfg(all(feature = "graphql", feature = "ws", feature = "explorer"))]
    let routes = ws_route.or(gql_route).or(expl_route);

    let port = config.web_port;
    let bind_addr = config.bind_addr;
    let cert_path = &config.cert_path;
    let key_path = &config.key_path;

    let server = warp::serve(routes);
    let addr = (bind_addr, port);

    if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
        log::info!("Using TLS certificate {}", cert_path);
        log::info!("Using TLS private key {}", key_path);
        log::info!("Starting web server with TLS …");

        server
            .tls()
            .cert_path(cert_path)
            .key_path(key_path)
            .run(addr)
            .await;
    } else {
        log::info!("Starting web server without TLS …");
        server.run(addr).await;
    }

    log::info!("Web server stopped.");
}

#[cfg(feature = "ws")]
async fn serve_ws(
    websocket: warp::ws::WebSocket,
    worterbuch: Arc<RwLock<Worterbuch>>,
    remote_addr: Option<SocketAddr>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();

    let (mut client_write, mut client_read) = websocket.split();

    spawn(async move {
        while let Some(bytes) = rx.recv().await {
            let msg = Message::binary(bytes);
            if let Err(e) = client_write.send(msg).await {
                log::error!("Error sending message to client: {e}");
                break;
            }
        }
    });

    let mut subscriptions = Vec::new();

    log::debug!("Receiving messages from client {remote_addr:?} …");
    loop {
        if let Some(Ok(incoming_msg)) = client_read.next().await {
            if incoming_msg.is_binary() {
                let data = incoming_msg.as_bytes();
                if !process_incoming_message(
                    data,
                    worterbuch.clone(),
                    tx.clone(),
                    &mut subscriptions,
                )
                .await?
                {
                    break;
                }
            }
        } else {
            break;
        }
    }
    log::debug!("No more messages from {remote_addr:?}, closing connection.");

    let mut wb = worterbuch.write().await;
    for subs in subscriptions {
        wb.unsubscribe(&subs.0, subs.1);
    }

    Ok(())
}
