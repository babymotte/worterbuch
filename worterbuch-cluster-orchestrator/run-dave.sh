#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/dave ./target/release/worterbuch-cluster-orchestrator -p 1340 dave
WORTERBUCH_LOG=info WORTERBUCH_TRACING=debug WORTERBUCH_DATA_DIR=./data/dave cargo run -- -p 1340 --stats-port 8387 dave
# docker run --rm --name wbco-dave -v $(pwd)/data/dave:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 1339:1339 wbco -p 1339 dave
