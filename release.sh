#!/bin/bash
cargo make test || exit 1
cargo ws version || exit 1
