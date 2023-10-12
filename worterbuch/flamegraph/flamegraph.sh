#!/bin/bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

CLI_DIR="$SCRIPT_DIR/../../worterbuch-cli"
BENCH_DIR="$SCRIPT_DIR/../benches"
TARGET_DIR="$SCRIPT_DIR/../../target/release"

cd "$CLI_DIR" && cargo build --release || exit 1
cd "$SCRIPT_DIR" && cargo build --release || exit 1

VALUES=$(jq <"$BENCH_DIR/dump.json" '.keyValuePairs | length')
echo Values in dump: $VALUES

sleep 1
flamegraph -- "$TARGET_DIR/worterbuch" &
sleep 3

export WORTERBUCH_PROTO=tcp
export WORTERBUCH_HOST_ADDRESS=localhost
export WORTERBUCH_PORT=8081

for i in {0..99}; do
    "$TARGET_DIR/wbpsub" -j '#' &>/dev/null &
done

time for i in {0..10}; do
    jq <"$BENCH_DIR/dump.json" -c '.keyValuePairs[]' | "$TARGET_DIR/wbset" -j >/dev/null
done

sleep 3
killall -s SIGINT "$TARGET_DIR/worterbuch"
echo
