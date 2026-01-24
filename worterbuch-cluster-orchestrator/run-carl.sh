#/bin/bash

WORTERBUCH_LOG=info WORTERBUCH_TRACING=debug WORTERBUCH_DATA_DIR=./data/carl WORTERBUCH_WS_SERVER_PORT=8280 WORTERBUCH_TCP_SERVER_PORT=9290 cargo run -- --stats-port 8385 -w ../target/debug/worterbuch carl
