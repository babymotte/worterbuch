#!/bin/bash
cargo install --path . --features ws,tcp,graphql
cargo install --path worterbuch-cli --features tcp
