#!/bin/bash
cargo make test-features || exit 1
cargo publish -p libworterbuch --dry-run || exit 1
