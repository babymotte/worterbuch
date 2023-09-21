#!/bin/bash

cargo build --release
flamegraph -- "../../target/release/worterbuch" &

sleep 1

export WORTERBUCH_HOST_ADDRESS=localhost
export WORTERBUCH_PORT=8080

for i in {0..99}; do wbpsub '#' &>/dev/null & done
time for i in {0..10}; do jq <"../benches/dump.json" -c '.keyValuePairs[]' | wbset -j >/dev/null; done

sleep 3
killall -s SIGINT "flamegraph"
