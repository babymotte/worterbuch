#!/bin/bash

cargo fmt --check &&
    cargo clippy -- --deny warnings &&
    cargo test
