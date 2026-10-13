#!/bin/bash

if [[ -n "$WBCLUSTER_UDP_CHECK_ADDRESS" ]]; then
    /usr/local/bin/udp-check "$@" || exit $?
fi

/usr/local/bin/worterbuch-cluster-orchestrator "$@"