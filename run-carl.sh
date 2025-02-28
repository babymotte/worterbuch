#/bin/bash

# RUST_LOG=info WORTERBUCH_DATA_DIR=./data/carl ./target/release/worterbuch-cluster-orchestrator -p 1339 carl
RUST_LOG=info WORTERBUCH_DATA_DIR=./data/carl cargo run -- -p 1339 --stats-port 8385 carl
# docker run --rm --name wbco-carl -v $(pwd)/data/carl:/data -v $(pwd)/config.yaml:/cfg/config.yaml -p 80:80 -p 9090:9090 -p 8283:8282 -p 1339:8181/udp docker.io/babymotte/worterbuch-cluster-orchestrator:1.1.1 carl
