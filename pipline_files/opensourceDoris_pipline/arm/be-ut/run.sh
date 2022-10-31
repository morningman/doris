#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc
set -ex

echo "####run be ut"
if bash run-be-ut.sh --clean --run -j 5; then
    # green
    echo -e "
\033[32m
##########################
run be ut, PASS
##########################
\033[0m"
    exit 0
else
    # red
    echo -e "
\033[31m
##########################
run be ut, FAIL
##########################
\033[0m"
    exit 1
fi
