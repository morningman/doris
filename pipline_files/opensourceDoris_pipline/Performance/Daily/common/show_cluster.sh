#!/bin/bash
set -ex

FE_HOST="172.21.0.4"
USER='root'
FE_QUERY_PORT='9030'

# wait_cluster_ready
show_cluster() {
    echo "frontends"
    mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e 'show frontends\G'
    echo "backends"
    mysql -h$FE_HOST -u$USER -P$FE_QUERY_PORT -e 'show backends\G'
}

show_cluster
