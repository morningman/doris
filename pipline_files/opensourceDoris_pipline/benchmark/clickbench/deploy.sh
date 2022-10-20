#!/bin/bash
set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%

if [[ ! -d output ]]; then echo "Can't find output dir, are you sure compiled?"; fi

DORIS_HOME="$teamcity_build_checkoutDir/output/"
echo "$DORIS_HOME" >doris_home

IPADDR=$(hostname -i)

echo "####change start fe config, longer time of networkaddress.cache.ttl"
sed -i 's/-XX:OnOutOfMemoryError/ -Dnetworkaddress.cache.ttl=100000 -XX:OnOutOfMemoryError/g' \
    "$DORIS_HOME"/fe/bin/start_fe.sh
tail -n 10 "$DORIS_HOME"/fe/bin/start_fe.sh

echo "####optimize doris config"
echo "
stream_load_default_timeout_second=3600
priority_networks = ${IPADDR}/24
" >"$DORIS_HOME"/fe/conf/fe_custom.conf

echo "
track_new_delete=false
streaming_load_max_mb=102400
doris_scanner_thread_pool_thread_num=8
tc_enable_aggressive_memory_decommit=false
enable_new_scan_node=false
mem_limit=95%
write_buffer_size=1609715200
load_process_max_memory_limit_percent=90
disable_auto_compaction=true
disable_storage_page_cache=false
disable_chunk_allocator=false
priority_networks = ${IPADDR}/24
" >"$DORIS_HOME"/be/conf/be_custom.conf

opt_session_variables="
exec_mem_limit=32G;
parallel_fragment_exec_instance_num=16;
enable_single_distinct_column_opt=true;
enable_function_pushdown=true;
enable_local_exchange=true;
"

# Start Frontend
"$DORIS_HOME"/fe/bin/start_fe.sh --daemon

# Start Backend
sudo sysctl -w vm.max_map_count=2000000
"$DORIS_HOME"/be/bin/start_be.sh --daemon

# wait for Doris FE ready
while true; do
    fe_version=$(mysql -h127.0.0.1 -P9030 -uroot -e 'show frontends' | cut -f16 | sed -n '2,$p')
    if [[ -n "${fe_version}" ]] && [[ "${fe_version}" != "NULL" ]]; then
        echo "fe version: ${fe_version}"
        mysql -h127.0.0.1 -P9030 -uroot -e 'admin show frontend config;' | grep 'write_buffer_size\|stream_load_default_timeout_second\|priority_networks'
        break
    else
        echo 'wait for Doris fe started.'
    fi
    sleep 2
done

# add BE to cluster
mysql -h 127.0.0.1 -P9030 -uroot -e "ALTER SYSTEM ADD BACKEND '${IPADDR}:9050' "

# wait for Doris BE ready
while true; do
    be_version=$(mysql -h127.0.0.1 -P9030 -uroot -e 'show backends' | cut -f22 | sed -n '2,$p')
    if [[ -n "${be_version}" ]]; then
        echo "be version: ${be_version}"
        curl '127.0.0.1:8040/varz' | grep 'load_process_max_memory_limit_percent\|chunk_reserved_bytes_limit\|storage_page_cache_limit\|streaming_load_max_mb\|doris_scanner_thread_pool_thread_num\|tc_enable_aggressive_memory_decommit\|enable_new_scan_node\|mem_limit\|disable_auto_compaction\|priority_networks'
        break
    else
        echo 'wait for Doris be started.'
    fi
    sleep 2
done

for session_variable in ${opt_session_variables}; do
    mysql -h 127.0.0.1 -P9030 -uroot -e "SET GLOBAL ${session_variable}"
done
mysql -h 127.0.0.1 -P9030 -uroot -e 'show variables' | grep 'load_mem_limit\|exec_mem_limit\|parallel_fragment_exec_instance_num\|enable_single_distinct_column_opt\|enable_function_pushdown\|enable_local_exchange'

echo "####deploy DONE."
