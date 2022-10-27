#!/bin/bash
set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%

if [[ ! -d output ]]; then echo "Can't find output dir, are you sure compiled?"; fi

DORIS_HOME="$teamcity_build_checkoutDir/output/"
echo "$DORIS_HOME" >doris_home

IPADDR=$(ifconfig -a | grep inet | grep -v 127.0.0.1 | grep -v inet6 | awk '{print $2}' | sed -n '$p')
IPADDR='127.0.0.1'

port_tail=$(bash port-tail-manager.sh 'take')
# FE port
http_port="803$port_tail"
rpc_port="902$port_tail"
query_port="903$port_tail"
edit_log_port="901$port_tail"
# BE port
be_port="906$port_tail"
webserver_port="804$port_tail"
heartbeat_service_port="905$port_tail"
brpc_port="806$port_tail"

echo "####set doris config"
echo "
http_port = $http_port
rpc_port = $rpc_port
query_port = $query_port
edit_log_port = $edit_log_port
priority_networks = ${IPADDR}/24
" >"$DORIS_HOME"/fe/conf/fe_custom.conf

echo "
be_port = $be_port
webserver_port = $webserver_port
heartbeat_service_port = $heartbeat_service_port
brpc_port = $brpc_port
priority_networks = ${IPADDR}/24
" >"$DORIS_HOME"/be/conf/be_custom.conf

opt_session_variables=""

# Start Frontend
"$DORIS_HOME"/fe/bin/start_fe.sh --daemon

# Start Backend
sudo sysctl -w vm.max_map_count=2000000
"$DORIS_HOME"/be/bin/start_be.sh --daemon

# wait for Doris FE ready
while true; do
    fe_version=$(mysql -h"$IPADDR" -P"$query_port" -uroot -e 'show frontends' | cut -f16 | sed -n '2,$p')
    if [[ -n "${fe_version}" ]] && [[ "${fe_version}" != "NULL" ]]; then
        echo "fe version: ${fe_version}"
        mysql -h"$IPADDR" -P"$query_port" -uroot -e 'admin show frontend config;'
        break
    else
        echo 'wait for Doris fe started.'
    fi
    sleep 2
done

# add BE to cluster
mysql -h"$IPADDR" -P"$query_port" -uroot -e "ALTER SYSTEM ADD BACKEND '${IPADDR}:${heartbeat_service_port}' "

# wait for Doris BE ready
while true; do
    be_version=$(mysql -h"$IPADDR" -P"$query_port" -uroot -e 'show backends' | cut -f22 | sed -n '2,$p')
    if [[ -n "${be_version}" ]]; then
        echo "be version: ${be_version}"
        curl "${IPADDR}:${webserver_port}/varz"
        break
    else
        echo 'wait for Doris be started.'
    fi
    sleep 2
done

for session_variable in ${opt_session_variables}; do
    mysql -h"$IPADDR" -P"$query_port" -uroot -e "SET GLOBAL ${session_variable}"
done
mysql -h"$IPADDR" -P"$query_port" -uroot -e 'show variables'

echo "####deploy DONE."
