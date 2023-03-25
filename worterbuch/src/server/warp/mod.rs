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

    let addr = (bind_addr, port);

    log::info!("Starting web server …");
    server.run(addr).await;

    log::info!("Web server stopped.");
}
