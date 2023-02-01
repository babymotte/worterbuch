pub(crate) mod common;
pub mod http;
pub mod ws;

use crate::{
    config::Config,
    server::{http::worterbuch_http_api_filter, ws::worterbuch_ws_filter},
    worterbuch::Worterbuch,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use warp::{Filter, Reply};

pub(crate) async fn start(worterbuch: Arc<RwLock<Worterbuch>>, config: Config) {
    log::info!("Starting Web Server …");

    let ws = worterbuch_ws_filter(worterbuch.clone());
    let http_api = worterbuch_http_api_filter(worterbuch.clone());
    let routes = http_api.or(ws);

    if config.webapp {
        let web_root_path = "*";
        let webpage = {
            log::info!("Mounting web app at {web_root_path} …");
            warp::fs::dir(config.web_root_path.clone())
        };
        let webapp_route = webpage;
        let routes = webapp_route.or(routes);
        run_server(routes, &config).await;
    } else {
        run_server(routes, &config).await;
    };
}

async fn run_server<F>(filter: F, config: &Config)
where
    F: Filter + Clone + Send + Sync + 'static,
    F::Extract: Reply,
{
    let server = warp::serve(filter);
    let port = config.port;
    let bind_addr = config.bind_addr;
    let cert_path = &config.cert_path;
    let key_path = &config.key_path;

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
