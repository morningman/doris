#!/bin/bash
set -ex

FE_HOST="172.21.0.4"
BE1_HOST="172.21.0.6"
BE2_HOST="172.21.0.8"
BE3_HOST="172.21.0.15"

# start_cluster <DEPLOY_DIR>
start_cluster() {
    DEPLOY_DIR="$1"

    echo "start fe"
    parallel-ssh -i -H${FE_HOST} "${DEPLOY_DIR}/fe/bin/start_fe.sh  --daemon"

    echo "start be"
    parallel-ssh -i -H${BE1_HOST} -H${BE2_HOST} -H${BE3_HOST} "ulimit -c unlimited \
    && ulimit -n 65535 \
    && sysctl -w vm.max_map_count=2000000 \
    && export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/ \
    && cd ${DEPLOY_DIR}/be && ./bin/start_be.sh --daemon"
}

if [[ -n "$1" ]]; then
    start_cluster "$1"
else
    echo "
Usage:
    $0 <DEPLOY_DIR>"
    exit 1
fi
