#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/alice target/release/worterbuch-cluster-orchestrator -H 10 -t 50 -p 1337 alice
RUST_LOG=info WORTERBUCH_DATA_DIR=./data/alice cargo run -- -p 1337 alice
