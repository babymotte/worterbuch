#/bin/bash

WORTERBUCH_LOG=info WORTERBUCH_TRACING=debug WORTERBUCH_DATA_DIR=./data/bob WORTERBUCH_WS_SERVER_PORT=8180 WORTERBUCH_TCP_SERVER_PORT=9190 cargo run -- --stats-port 8384 -w ../target/debug/worterbuch bob
