#!/bin/bash

cargo clippy &&
    cargo clippy --features=commercial &&
    cargo test &&
    cargo test --features=commercial
