#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <port>"
    exit 1
fi
watch -n 0.5 'ss -tino "( sport = :$1 )"'
