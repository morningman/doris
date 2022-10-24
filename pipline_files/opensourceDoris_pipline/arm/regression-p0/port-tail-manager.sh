#!/bin/bash
# This script is used to assign port tail in order to run muti doris clusters on one machine.
# return free port tail, port from 9030 to 9039, so tail from 0 to 9
set -ex

teamcity_home=${HOME}/teamcity

port_tail_file="${teamcity_home}/busy_ports"

take_port_tail() {
    row_count=$(awk 'END{print NR}' "$port_tail_file")
    if [ "$row_count" = 10 ]; then
        echo 'port tail all busy!!!'
        exit 1
    fi

    for i in {0..9}; do
        if ! grep "$i" "$port_tail_file" >/dev/null; then
            echo "$i" | tee -a "$port_tail_file"
            return
        fi
    done
}

return_port_tail() {
    port_tail="$1"
    if ! grep "$port_tail" "$port_tail_file"; then
        echo "can not fine port tail $port_tail in $port_tail_file!!!"
        exit 1
    fi
    sed -i "/$port_tail/d" "$port_tail_file"
}

check_port_tail() {
    if [[ -f "$port_tail_file" ]]; then
        port_tails=$(cat "$port_tail_file")
        for port_tail in $port_tails; do
            # if 903$port_tail port id not in using, remove $port_tail from $port_tail_file
            if ! lsof -i:"903$port_tail"; then sed -i "/$port_tail/d" "$port_tail_file"; fi
        done
    fi
}

if [[ "$1" == "check" ]]; then
    check_port_tail
elif [[ "$1" == "take" ]]; then
    take_port_tail
elif [[ "$1" == "return" ]]; then
    return_port_tail "$2"
else
    echo "
Usage:
    $0 check
    $0 take
    $0 return [0-9]"
    exit 1
fi
