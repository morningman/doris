#!/bin/bash
set -ex

FE_HOST="172.21.0.4"
BE1_HOST="172.21.0.6"
BE2_HOST="172.21.0.8"
BE3_HOST="172.21.0.15"

# stop_cluster <DEPLOY_DIR>
stop_cluster() {
    DEPLOY_DIR="$1"
    echo "stop fe"
    parallel-ssh -i -H${FE_HOST} "${DEPLOY_DIR}/fe/bin/stop_fe.sh"
    echo "stop be"
    parallel-ssh -i -H${BE1_HOST} -H${BE2_HOST} -H${BE3_HOST} "${DEPLOY_DIR}/be/bin/stop_be.sh"
    set +e
    parallel-ssh -i -H${FE_HOST} -H${BE1_HOST} -H${BE2_HOST} -H${BE3_HOST} "pgrep PaloFe   | xargs sudo kill -9"
    parallel-ssh -i -H${FE_HOST} -H${BE1_HOST} -H${BE2_HOST} -H${BE3_HOST} "pgrep doris_be | xargs sudo kill -9"
    set -e
}

if [[ -n "$1" ]]; then
    stop_cluster "$1"
else
    echo "
Usage:
    $0 <DEPLOY_DIR>"
    exit 1
fi
