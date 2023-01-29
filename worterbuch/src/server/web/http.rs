use crate::worterbuch::Worterbuch;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use urlencoding::decode;
use warp::path::Tail;
use warp::reject::Reject;
use warp::{reject, Filter, Rejection, Reply};
use worterbuch_common::error::WorterbuchError;
use worterbuch_common::KeyValuePair;

#[derive(Debug)]
struct WorterbuchErrorRejection {
    _error: WorterbuchError,
}

impl From<WorterbuchError> for WorterbuchErrorRejection {
    fn from(_error: WorterbuchError) -> Self {
        WorterbuchErrorRejection { _error }
    }
}

impl Reject for WorterbuchErrorRejection {}

pub fn worterbuch_http_api_filter(
    worterbuch: Arc<RwLock<Worterbuch>>,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone + Send + Sync + 'static {
    let api_path = "api";

    log::info!("Mounting ws endpoint at /{api_path} …");

    let get_wb = worterbuch.clone();
    let pget_wb = worterbuch.clone();
    let set_wb = worterbuch.clone();

    let get = {
        warp::get()
            .and(warp::path(api_path))
            .and(warp::path("get"))
            .and(warp::path::tail())
            .and(warp::query::<HashMap<String, String>>())
            .and_then(move |tail: Tail, query: HashMap<String, String>| {
                let worterbuch = get_wb.clone();
                async move {
                    let key = match decode(tail.as_str()) {
                        Ok(it) => it.to_string(),
                        Err(_) => tail.as_str().to_string(),
                    };
                    log::debug!("Processing http request 'get {key}'");
                    let raw = query.contains_key("raw")
                        && query.get("raw").map(|s| s.to_lowercase()) != Some("false".to_owned());
                    match worterbuch.read().await.get(key) {
                        Ok((key, value)) => {
                            if raw {
                                Ok(value)
                            } else {
                                Ok(serde_json::to_string(&KeyValuePair { key, value })
                                    .expect("cannot fail"))
                            }
                        }
                        Err(e) => Err(reject::custom(WorterbuchErrorRejection::from(e))),
                    }
                }
            })
    };

    let pget = {
        warp::get()
            .and(warp::path(api_path))
            .and(warp::path("pget"))
            .and(warp::path::tail())
            .and_then(move |tail: Tail| {
                let worterbuch = pget_wb.clone();
                async move {
                    let pattern = match decode(tail.as_str()) {
                        Ok(it) => it.to_string(),
                        Err(_) => tail.as_str().to_string(),
                    };
                    log::debug!("Processing http request 'pget {pattern}'");
                    match worterbuch.read().await.pget(&pattern) {
                        Ok(pstate) => Ok(serde_json::to_string(&pstate).expect("cannot fail")),
                        Err(e) => Err(reject::custom(WorterbuchErrorRejection::from(e))),
                    }
                }
            })
    };

    let set = {
        warp::post()
            .and(warp::path(api_path))
            .and(warp::path("set"))
            .and(warp::filters::body::form())
            .and_then(move |data: HashMap<String, String>| {
                let worterbuch = set_wb.clone();
                async move {
                    let mut wb = worterbuch.write().await;
                    for (key, value) in data {
                        log::debug!("Processing http request 'set {key} = {value}'");
                        if let Err(e) = wb.set(key, value) {
                            return Err(reject::custom(WorterbuchErrorRejection::from(e)));
                        }
                    }
                    Ok("Ok".to_owned())
                }
            })
    };

    get.or(pget).or(set)
}
