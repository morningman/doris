#!/bin/bash
set -ex

teamcity_pullRequest_number=%teamcity.pullRequest.number%
build_id=%teamcity.build.id%

ts=$(date '+%Y%m%d%H%M%S')
html_file_name="${ts}_clickbench_pr_${build_id}.html"

echo '-------------------------------------------------------------'
echo "####record some info"
bash get-doris-runtime-conf.sh
echo '-------------------------------------------------------------'
bash get-table-schema.sh
echo '-------------------------------------------------------------'
bash get-mechine-info.sh
echo '-------------------------------------------------------------'
bash get-ckb-html.sh | tee "$html_file_name"
echo '-------------------------------------------------------------'

echo "####upload $html_file_name to cos"
set +x
# shellcheck source=/dev/null
source ~/.bashrc
set -x
coscli cp \
    "$html_file_name" \
    cos://doris-community-test-1308700295/tmp/"$html_file_name"

set +x
echo "####create an issue comment on github"
bash create-an-issue-comment-on-github.sh \
    "$teamcity_pullRequest_number" \
    "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/$html_file_name"

echo -e "
=============================================================

wget the html below then open it with browser

wget https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/$html_file_name

=============================================================
"
