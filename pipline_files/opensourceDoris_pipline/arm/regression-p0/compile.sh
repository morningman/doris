#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

teamcity_agent_home_dir=%teamcity.agent.home.dir%
teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
teamcity_pullRequest_number=%teamcity.pullRequest.number%
teamcity_pullRequest_source_branch=%teamcity.pullRequest.source.branch%
teamcity_pullRequest_target_branch=%teamcity.pullRequest.target.branch%
build_id=%teamcity.build.id%

tmp_env_file_path="$teamcity_build_checkoutDir/.my_tmp_env"
doris_thirdparty_dir="$teamcity_agent_home_dir/doris_thirdparty"
build_thirdparty=${build_thirdparty:="false"}
skip_pipeline=${skip_pipeline:="false"}
teamcity_home=${HOME}/teamcity/
REPOSITORY_URL='https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/thirdparty'
REPOSITORY_URL='https://doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com/thirdparty'

# for ccache-3.7.7
CCACHE_DIR=$(ccache -s | grep 'cache directory' | awk '{print $3}')
# for ccache-4.7
if [[ -z $CCACHE_DIR ]]; then CCACHE_DIR=$(ccache -sv | grep -i 'cache directory' | awk '{print $3}'); fi

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
if bash check_change_file.sh --is_modify_only_invoved_doc $teamcity_pullRequest_number 2>/dev/null; then
    # if no need to run this pipeline, use env to tell next steps
    echo "export skip_pipeline='true'" >"$tmp_env_file_path" && exit 0
fi
set -e
echo -e "FINISH check!\n"

echo "####record build info, use to show what pr has triggered build"
echo "$build_id ${build_record_item}" >>"$teamcity_home"/OpenSourceDorisBuild.log

git branch

echo "####configure build"
echo -e "
export USE_AVX2=OFF
export DORIS_TOOLCHAIN=gcc
export BUILD_TYPE=release
export REPOSITORY_URL='$REPOSITORY_URL'
export DORIS_THIRDPARTY='$doris_thirdparty_dir'
" >"$teamcity_build_checkoutDir"/custom_env.sh
if [[ ! -d $doris_thirdparty_dir ]]; then mkdir -p "$doris_thirdparty_dir"; fi
# update thirdparty
cp -rf "$teamcity_build_checkoutDir"/thirdparty/* "$doris_thirdparty_dir"
if [[ "${build_thirdparty}" == "true" ]]; then
    echo "rm -rf $doris_thirdparty_dir/* and build doris thirdparty"
    rm -rf "${doris_thirdparty_dir:?}"/*
fi

echo "####build Doris"
cd "$teamcity_build_checkoutDir"
bash build.sh -j"$(($(nproc) / 2))" | tee build.log

echo "####check build result"
succ_symble="BUILD SUCCESS"
grep "$succ_symble" "$teamcity_build_checkoutDir"/build.log
#check output is exist or not
if [ ! -d output ]; then
    echo -e "\e[1;31m BUILD FAIL, NO OUTPUT \e[40;37m"
    echo "clean working dir"
    cd "$teamcity_build_checkoutDir"
    # sudo rm -rf !(build.log) #TODO unsupport???
    ls | grep -v 'build.log' | xargs rm -rf
    exit 1
fi

sudo chown -R "$USER":"$USER" output/

echo "####compile DONE."
