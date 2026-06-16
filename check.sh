#!/bin/bash

echo "cargo fmt --check" && cargo fmt --check &&
    echo "cargo clippy -- --deny warnings" && cargo clippy -- --deny warnings &&
    echo "cargo clippy --no-default-features -- --deny warnings" && cargo clippy --no-default-features -- --deny warnings &&
    echo "cargo clippy --no-default-features --features=ws -- --deny warnings" && cargo clippy --no-default-features --features=ws -- --deny warnings &&
    echo "cargo clippy --features=commercial -- --deny warnings" && WORTERBUCH_PUBLIC_KEY_FILE=./tests/key.asc WORTERBUCH_LICENSE_FILE=./tests/license cargo clippy --features=commercial -- --deny warnings &&
    echo "cargo test" && cargo test &&
    echo "cargo test --no-default-features" && cargo test --no-default-features &&
    echo "cargo test --features=commercial" && WORTERBUCH_PUBLIC_KEY_FILE=./tests/key.asc WORTERBUCH_LICENSE_FILE=./tests/license cargo test --features=commercial &&
    echo "cargo test --no-default-features --features=commercial" && WORTERBUCH_PUBLIC_KEY_FILE=./tests/key.asc WORTERBUCH_LICENSE_FILE=./tests/license cargo test --no-default-features --features=commercial
