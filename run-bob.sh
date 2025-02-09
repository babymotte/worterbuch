#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/bob target/release/worterbuch-cluster-orchestrator -H 10 -t 50 -p 1338 bob
RUST_LOG=info WORTERBUCH_DATA_DIR=./data/bob cargo run -- -p 1338 bob
