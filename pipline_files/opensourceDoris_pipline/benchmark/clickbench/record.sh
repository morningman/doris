#!/bin/bash
set -ex

build_id=%teamcity.build.id%

echo '-------------------------------------------------------------'
bash get-doris-runtime-conf.sh
echo '-------------------------------------------------------------'
bash get-table-schema.sh
echo '-------------------------------------------------------------'
bash get-mechine-info.sh
echo '-------------------------------------------------------------'
set +e
bash check-result.sh
set -e
echo '-------------------------------------------------------------'
bash get-ckb-html.sh | tee "clickbench_pr_${build_id}.html"
echo '-------------------------------------------------------------'

coscli cp \
    "clickbench_pr_${build_id}.html" \
    cos://doris-community-test-1308700295/tmp/"clickbench_pr_${build_id}.html"

echo '============================================================='
echo https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/"clickbench_pr_${build_id}.html"
echo '============================================================='
