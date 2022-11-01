#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc
source ~/.bashrc_tmp

set -ex

teamcity_agent_home_dir=%teamcity.agent.home.dir%
teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
# teamcity_agent_home_dir='/root/teamcity/teamcity-agent-0'
# teamcity_build_checkoutDir='/root/teamcity/teamcity-agent-0/work/e0bc234628561cad'

doris_thirdparty_dir="$teamcity_agent_home_dir/doris_thirdparty"
build_thirdparty=${build_thirdparty:="false"}
REPOSITORY_URL='https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/thirdparty'
# REPOSITORY_URL='https://doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com/thirdparty'

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
