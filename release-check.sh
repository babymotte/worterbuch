#!/bin/bash
cargo make test || exit 1
cargo publish -p libworterbuch --dry-run || exit 1
