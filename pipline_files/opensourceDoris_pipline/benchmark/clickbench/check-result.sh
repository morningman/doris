#!/bin/bash
# set -x

DORIS_HOME=$(cat doris_home)

set +e
# ./kill-doris-cluster.sh

# # Start Frontend
# "$DORIS_HOME"/fe/bin/start_fe.sh --daemon

# # Start Backend
# sudo sysctl -w vm.max_map_count=2000000
# "$DORIS_HOME"/be/bin/start_be.sh --daemon

# sleep 30

if [[ ! -d doris-result ]]; then mkdir doris-result; fi

QUERY_NUM=1
while read -r query; do
    echo "$query"
    mysql -h:: -P9030 -uroot -Dhits -e"$query" >doris-result/doris-q${QUERY_NUM}.result
    QUERY_NUM=$((QUERY_NUM + 1))
done <queries-sort.sql

cd ../query-result-target/ || exit
for i in {1..43}; do
    echo
    echo "query$i"
    sed -n '2,$p' "../doris/doris-result/doris-q$i.result" >"../doris/doris-result/doris-q$i.result2"
    # echo "diff -w \"ck-q$i.result\" \"../doris/doris-result/doris-q$i.result2\""
    diff -w "ck-q$i.result" "../doris/doris-result/doris-q$i.result2"
    # read a
done
