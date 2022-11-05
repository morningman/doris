#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc
set -e

teamcity_pullRequest_number=%teamcity.pullRequest.number%
build_id=%teamcity.build.id%
teamcity_build_checkoutDir=%teamcity.build.checkoutDir%

skip_pipeline=${skip_pipeline:="false"}
tmp_env_file_path="$teamcity_build_checkoutDir/.my_tmp_env"
if [[ -f $"$tmp_env_file_path" ]]; then source "$tmp_env_file_path"; fi
echo '####check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then echo "skip build pipline" && exit 0; fi

# TeamCity treats a string surrounded by percentage signs (%) in the script as a parameter reference.
# To prevent TeamCity from treating the text in the percentage signs as a property reference,
# use double percentage signs to escape them.
ts=$(date '+%%Y%%m%%d%%H%%M%%S')
html_file_name="${ts}_clickbench_pr_${build_id}.html"

echo '-------------------------------------------------------------'
# echo "####record some info"
# bash get-doris-runtime-conf.sh
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
sum_best_hot_time=$(awk -F ',' '{if($3<$4){sum+=$3}else{sum+=$4}} END {print sum}' result.csv)
loadtime=$(cat loadtime)
storage_size=$(cat storage_size)
comment_body="TeamCity pipeline, clickbench performance test result:\n the sum of best hot time: $sum_best_hot_time seconds\n load time: $loadtime seconds\n storage size: $storage_size Bytes\n https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/$html_file_name"
bash update-or-create-an-issue-comment.sh \
    "$teamcity_pullRequest_number" \
    "$comment_body"

echo -e "
=============================================================

wget the html below then open it with browser

wget https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/tmp/$html_file_name

=============================================================
"
