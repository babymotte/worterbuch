#!/bin/bash

cargo fmt --check &&
    cargo clippy &&
    cargo test
