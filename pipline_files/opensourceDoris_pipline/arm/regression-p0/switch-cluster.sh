#!/bin/bash

set -x

# Doris cluster manager manage cluster stop

usage() {
    echo "
Usage:
    $0 <DORIS_HOME> [on|off]"
    exit 1
}

switch_on() {
    echo 'Not implement'
    exit 1
}

switch_off() {
    local doris_home="$1"
    echo 'try to use stop_fe.sh and stop_be.sh to stop doris cluster'
    bash "${doris_home}/fe/bin/stop_fe.sh"
    bash "${doris_home}/be/bin/stop_be.sh"

    echo 'try to kill FE and BE according to the port in conf file'

    if [[ -f "$doris_home"/fe/conf/fe_custom.conf ]]; then
        fe_conf_query_port=$(grep query_port "$doris_home"/fe/conf/fe_custom.conf | sed 's/ //g' | cut -d'=' -f2)
    else
        fe_conf_query_port=$(grep query_port "$doris_home"/fe/conf/fe.conf | sed 's/ //g' | cut -d'=' -f2)
    fi
    if lsof -i:"$fe_conf_query_port"; then
        # COMMAND   PID       USER   FD   TYPE     DEVICE SIZE/OFF NODE NAME
        pid=$(lsof -i:"$fe_conf_query_port" | sed -n '2p' | awk '{print $2}')
        kill -9 "$pid"
    fi

    if [[ -f "$doris_home"/be/conf/be_custom.conf ]]; then
        be_conf_be_port=$(grep be_port "$doris_home"/be/conf/be_custom.conf | sed 's/ //g' | cut -d'=' -f2)
    else
        be_conf_be_port=$(grep be_port "$doris_home"/be/conf/be.conf | sed 's/ //g' | cut -d'=' -f2)
    fi
    if lsof -i:"$be_conf_be_port"; then
        # COMMAND   PID       USER   FD   TYPE     DEVICE SIZE/OFF NODE NAME
        pid=$(lsof -i:"$be_conf_be_port" | sed -n '2p' | awk '{print $2}')
        kill -9 "$pid"
    fi

    echo 'return this free port to port-tail-manager'
    port_tail=${fe_conf_query_port:0-1}
    bash port-tail-manager.sh return "$port_tail"

    exit 0
}

if [[ $# != 2 ]]; then
    usage
elif [[ $2 == 'on' ]]; then
    switch_on "$1"
elif [[ $2 == 'off' ]]; then
    switch_off "$1"
else
    usage
fi
