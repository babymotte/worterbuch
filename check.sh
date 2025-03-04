#!/bin/bash

cargo fmt --check &&
    cargo clippy -- --deny warnings &&
    cargo clippy --features=commercial -- --deny warnings &&
    cargo test &&
    cargo test --features=commercial
