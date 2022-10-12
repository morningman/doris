#!/bin/bash
set -ex

echo "####stop doris"
set +e
bash kill-doris-cluster.sh
set -e

echo "####clean DONE."
