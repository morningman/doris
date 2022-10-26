#!/bin/bash
set -ex

echo "####stop doris"
set +e
bash switch-cluster.sh off
set -e

echo "####clean DONE."
