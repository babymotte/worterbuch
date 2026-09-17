#/bin/bash

WORTERBUCH_TOKIO_CONSOLE_PORT=7777 \
WORTERBUCH_DATA_DIR=./data/bob \
WORTERBUCH_WS_SERVER_PORT=8081 \
WORTERBUCH_TCP_SERVER_PORT=9091 \
cargo run -- --stats-port 7071 -w ../target/debug/worterbuch bob
