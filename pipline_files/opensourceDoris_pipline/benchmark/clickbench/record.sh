#!/bin/bash
set -ex

teamcity_pullRequest_number=%teamcity.pullRequest.number%
build_id=%teamcity.build.id%

echo '-------------------------------------------------------------'
echo "####record some info"
bash get-doris-runtime-conf.sh
echo '-------------------------------------------------------------'
bash get-table-schema.sh
echo '-------------------------------------------------------------'
bash get-mechine-info.sh
echo '-------------------------------------------------------------'
bash get-ckb-html.sh | tee "clickbench_pr_${build_id}.html"
echo '-------------------------------------------------------------'

echo "####upload clickbench.html to cos"
set +x
# shellcheck source=/dev/null
source ~/.bashrc
set -x
coscli cp \
    "clickbench_pr_${build_id}.html" \
    cos://doris-community-test-1308700295/tmp/"clickbench_pr_${build_id}.html"

set +x
echo "####create an issue comment on github"
bash create-an-issue-comment-on-github.sh \
    "$teamcity_pullRequest_number" \
    "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/clickbench_pr_${build_id}.html"

echo -e "
=============================================================

wget the html below then open it with browser

wget https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/clickbench_pr_${build_id}.html

=============================================================
"
