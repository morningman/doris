#!/bin/bash
# set -x

set +e
# ./kill-doris-cluster.sh

# DORIS_HOME=$(cat doris_home)
# # Start Frontend
# "$DORIS_HOME"/fe/bin/start_fe.sh --daemon

# # Start Backend
# sudo sysctl -w vm.max_map_count=2000000
# "$DORIS_HOME"/be/bin/start_be.sh --daemon

# sleep 30

if [[ ! -d query-result-actual ]]; then mkdir query-result-actual; fi

QUERY_NUM=1
while read -r query; do
    echo "$query"
    mysql -h:: -P9030 -uroot -Dhits -e"$query" >query-result-actual/doris-q${QUERY_NUM}.result
    QUERY_NUM=$((QUERY_NUM + 1))
done <queries-sort.sql

is_ok=true
for i in {1..43}; do
    echo "####use diff command to check query$i"
    if ! diff -w "query-result-target/doris-q$i.result" "query-result-actual/doris-q$i.result"; then
        is_ok=false
    fi
done

if $is_ok; then exit 0; else exit 1; fi
