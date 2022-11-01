#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

teamcity_agent_home_dir=%teamcity.agent.home.dir%
teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
teamcity_pullRequest_number=%teamcity.pullRequest.number%
teamcity_pullRequest_source_branch=%teamcity.pullRequest.source.branch%
teamcity_pullRequest_target_branch=%teamcity.pullRequest.target.branch%
# teamcity_agent_home_dir='/root/teamcity/teamcity-agent-0'
# teamcity_build_checkoutDir='/root/teamcity/teamcity-agent-0/work/e0bc234628561cad'

teamcity_home=${HOME}/teamcity/
doris_thirdparty_dir="$teamcity_agent_home_dir/doris_thirdparty"
build_thirdparty=${build_thirdparty:="false"}
skip_pipeline=${skip_pipeline:="false"}
REPOSITORY_URL='https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/thirdparty'
REPOSITORY_URL='https://doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com/thirdparty'

tmp_env_file_path="$teamcity_build_checkoutDir/.my_tmp_env"
if [[ -f $"$tmp_env_file_path" ]]; then source "$tmp_env_file_path"; fi

echo '####check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then echo "skip build pipline" && exit 0; fi

echo "####check if old build of same pr still running, cancel it if so"
build_record_item=${teamcity_pullRequest_number}_${teamcity_pullRequest_source_branch}_${teamcity_pullRequest_target_branch}_doris
while read -r old_build_id; do
    echo "STRAT checking build ${old_build_id}"
    old_build_status=$(bash teamcity_api.sh --show_build_state "${old_build_id}")
    if [[ ${old_build_status} == "running" ]]; then
        bash teamcity_api.sh --cancel_running_build "${old_build_id}"
    fi
done < <(grep ${build_record_item} "$teamcity_home"/OpenSourceDorisBuild.log | awk '{print $1}')

#skip build which trigered by file under docs/zh-CN/docs/sql-manual/
echo "###check change file to see if need run this pipeline"
set +e
if bash check_change_file.sh --is_modify_only_invoved_doc \
    $teamcity_pullRequest_number 2>/dev/null; then exit 0; fi
set -e
echo -e "FINISH check!\n"

echo "####check if need build thirdparty"
if [[ ! -d $doris_thirdparty_dir ]]; then mkdir -p "$doris_thirdparty_dir"; fi
if [[ "${build_thirdparty}" == "true" ]]; then
    export REPOSITORY_URL="${REPOSITORY_URL}"
    if bash thirdparty/build-thirdparty.sh -j"$(($(nproc) / 2))"; then
        echo "build thirdparty done, then replace old thirdparty to new"
        rm -rf "${doris_thirdparty_dir:?}"/*
        cp -rf "$teamcity_build_checkoutDir"/thirdparty/* "$doris_thirdparty_dir"
    else
        echo "build thirdparty fail..." && exit 1
    fi
fi
echo -e "
export USE_AVX2=OFF
export DORIS_TOOLCHAIN=gcc
export DORIS_THIRDPARTY='$doris_thirdparty_dir'
" >"$teamcity_build_checkoutDir"/custom_env.sh

echo "####run be ut"
if bash run-be-ut.sh --clean --run -j"$(($(nproc) / 2))" &&
    [[ -d "$teamcity_build_checkoutDir"/be/ut_build_ASAN/gtest_output ]]; then
    # green
    echo -e "
\033[32m
##########################
run be ut, PASS
##########################
\033[0m" && exit 0
else
    # red
    echo -e "
\033[31m
##########################
run be ut, FAIL
##########################
\033[0m" && exit 1
fi
