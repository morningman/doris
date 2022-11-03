#!/bin/bash
set -e

echo "####stop doris"
set +e
bash kill-doris-cluster.sh
set -e

echo "####clean DONE."
