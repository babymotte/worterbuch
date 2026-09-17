#/bin/bash

WORTERBUCH_TOKIO_CONSOLE_PORT=8888 \
WORTERBUCH_DATA_DIR=./data/carl \
WORTERBUCH_WS_SERVER_PORT=8082 \
WORTERBUCH_TCP_SERVER_PORT=9092 \
cargo run -- --stats-port 7072 -w ../target/debug/worterbuch carl
