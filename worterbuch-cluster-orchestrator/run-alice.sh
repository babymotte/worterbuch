#/bin/bash

WORTERBUCH_DATA_DIR=./data/alice WORTERBUCH_WS_SERVER_PORT=8080 WORTERBUCH_TCP_SERVER_PORT=9090 cargo run -- --stats-port 7070 -w ../target/debug/worterbuch alice
