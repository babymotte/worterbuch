#/bin/bash

WORTERBUCH_LOG=info WORTERBUCH_TRACING=debug WORTERBUCH_DATA_DIR=./data/alice WORTERBUCH_WS_SERVER_PORT=8080 WORTERBUCH_TCP_SERVER_PORT=9090 cargo run -- --stats-port 8383 -w ../target/debug/worterbuch alice
