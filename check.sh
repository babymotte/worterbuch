#!/bin/bash

cargo fmt --check &&
    cargo clippy &&
    cargo clippy --features=commercial &&
    cargo test &&
    cargo test --features=commercial
