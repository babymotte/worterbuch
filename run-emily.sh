#/bin/bash

RUST_LOG=info WORTERBUCH_DATA_DIR=./data/emily ./target/release/worterbuch-cluster-orchestrator -p 1341 emily
# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/carl cargo run -- -p 1339 --stats-port 8387 carl
# docker run --rm --name wbco-carl -v $(pwd)/data/carl:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 1339:1339 wbco -p 1339 carl
