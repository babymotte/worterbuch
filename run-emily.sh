#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/emily ./target/release/worterbuch-cluster-orchestrator -p 1341 emily
RUST_LOG=info WORTERBUCH_DATA_DIR=./data/emily cargo run -- -p 1341 --stats-port 8388 emily
# docker run --rm --name wbco-emily -v $(pwd)/data/emily:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 1339:1339 wbco -p 1339 carl
