#!/bin/bash

cargo fmt --check &&
    cargo clippy -- --deny warnings &&
    cargo clippy --no-default-features -- --deny warnings &&
    cargo clippy --no-default-features --features=ws -- --deny warnings &&
    WORTERBUCH_PUBLIC_KEY_FILE=./tests/key.asc WORTERBUCH_LICENSE_FILE=./tests/license cargo clippy --features=commercial -- --deny warnings &&
    cargo test &&
    cargo test --no-default-features &&
    WORTERBUCH_PUBLIC_KEY_FILE=./tests/key.asc WORTERBUCH_LICENSE_FILE=./tests/license cargo test --features=commercial
