#!/bin/bash
set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
teamcity_pullRequest_number=%teamcity.pullRequest.number%
teamcity_pullRequest_source_branch=%teamcity.pullRequest.source.branch%
teamcity_pullRequest_target_branch=%teamcity.pullRequest.target.branch%
build_id=%teamcity.build.id%
# build_vcs_number=%build.vcs.number%

teamcity_home=${HOME}/teamcity/
# for ccache-3.7.7
CCACHE_DIR=$(ccache -s | grep 'cache directory' | awk '{print $3}')
# for ccache-4.7
if [[ -z $CCACHE_DIR ]]; then CCACHE_DIR=$(ccache -sv | grep -i 'cache directory' | awk '{print $3}'); fi

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
    exit 0
fi
set -e
echo -e "FINISH check!\n"

echo "####record build info, use to show what pr has triggered build"
echo "$build_id ${build_record_item}" >>"$teamcity_home"/OpenSourceDorisBuild.log

git branch

echo "####config build"
echo -e "
export DORIS_TOOLCHAIN=gcc
export BUILD_TYPE=release
" >"$teamcity_build_checkoutDir"/custom_env.sh
echo -e"
    replace 
    REPOSITORY_URL=
    to
    REPOSITORY_URL=https://doris-thirdparty-1308700295.cos.ap-beijing.myqcloud.com/thirdparty
    in
    thirdparty/vars.sh"
sed -i "s/export REPOSITORY_URL=/export REPOSITORY_URL=https:\/\/doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com\/thirdparty/g" \
    thirdparty/vars.sh
# TODO: compile libhdfs add -DENABLE_SSE=OFF

echo "####build Doris"
cd "$teamcity_build_checkoutDir"
bash build.sh | tee build.log

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
