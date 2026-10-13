#!/bin/bash

if [[ -n "$WBCLUSTER_STARTUP_DELAY" ]]; then
    echo "Delaying startup by $WBCLUSTER_STARTUP_DELAY seconds …"
    sleep "$WBCLUSTER_STARTUP_DELAY"
fi

/usr/local/bin/worterbuch-cluster-orchestrator "$@"