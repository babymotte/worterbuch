use crate::{
    server::graphql::{self, Context},
    worterbuch::Worterbuch,
};
use juniper_graphql_ws::ConnectionConfig;
use juniper_warp::subscriptions::serve_graphql_ws;
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::Filter;
use worterbuch::config::Config;

pub(crate) async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) {
    log::info!("Starting Web Server...");

    let graphiql = warp::get()
        .and(warp::path("graphiql"))
        .and(juniper_warp::graphiql_filter("/graphql", Some("/ws")));

    let context = Context {
        database: worterbuch,
    };
    let ws_schema = Arc::new(graphql::schema());
    let ws_context = context.clone();
    let graphql_schema = graphql::schema();
    let state = warp::any().map(move || context.clone());
    let graphql_filter = juniper_warp::make_graphql_filter(graphql_schema, state.boxed());
    let graphql = warp::path("graphql").and(graphql_filter);

    let graphql_ws = warp::path("ws")
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

    let routes = graphql.or(graphql_ws).or(graphiql);

    let port = config.graphql_port;
    let bind_addr = config.bind_addr;
    let cert_path = &config.cert_path;
    let key_path = &config.key_path;

    let server = warp::serve(routes);
    let addr = (bind_addr, port);

    if let (Some(cert_path), Some(key_path)) = (cert_path, key_path) {
        log::info!("Using TLS certificate {}", cert_path);
        log::info!("Using TLS private key {}", key_path);
        log::info!("Starting web server with TLS...");

        server
            .tls()
            .cert_path(cert_path)
            .key_path(key_path)
            .run(addr)
            .await;
    } else {
        log::info!("Starting web server without TLS...");
        server.run(addr).await;
    }

    log::info!("Web server stopped.");
}
