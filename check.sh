#!/bin/bash

cargo fmt --check &&
    cargo clippy -- --deny warnings &&
    cargo clippy --no-default-features -- --deny warnings &&
    cargo clippy --no-default-features --features=ws -- --deny warnings &&
    cargo clippy --features=commercial -- --deny warnings &&
    cargo test &&
    cargo test --no-default-features &&
    cargo test --features=commercial
