#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/carl target/release/worterbuch-cluster-orchestrator -H 10 -t 50 -p 1339 carl
RUST_LOG=info WORTERBUCH_DATA_DIR=./data/carl cargo run -- -p 1339 carl
