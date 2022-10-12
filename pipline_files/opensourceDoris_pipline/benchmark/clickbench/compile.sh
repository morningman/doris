#!/bin/bash
set -ex

pipeline_home=${HOME}/teamcity/
curdir=%teamcity.build.checkoutDir%
pullrequestID=%teamcity.pullRequest.number%
source_branch=%teamcity.pullRequest.source.branch%
target_branch=%teamcity.pullRequest.target.branch%
build_id=%teamcity.build.id%

echo 'step 1, compile'

# cp -r "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/common/* ./
# cp "${pipeline_home}"/check_change_file.sh ./

outdate_builds_of_pr=($(grep ${pullrequestID}_${source_branch}_${target_branch}_incubator-doris $pipeline_home/OpenSourceDorisBuild.log | awk '{print $1}'))
for old_build_id in ${outdate_builds_of_pr[@]}; do
    echo "STRAT checking build ${old_build_id}"
    old_build_status=$(bash teamcity_api.sh --show_build_state "${old_build_id}")
    if [[ ${old_build_status} == "running" ]]; then
        bash teamcity_api.sh --cancel_running_build "${old_build_id}"
    fi
done

#skip build which trigered by file under docs/zh-CN/docs/sql-manual/
echo "check change file"
set +e
bash check_change_file.sh --is_modify_only_invoved_doc %teamcity.pullRequest.number% 2>/dev/null
if [[ $? == 0 ]]; then
    exit 0
fi
set -e
echo -e "FINISH check!\n"

#recoding itself build info
echo "$build_id ${pullrequestID}_${source_branch}_${target_branch}_doris" >>"$pipeline_home"/OpenSourceDorisBuild.log

git branch

echo -e "
export DORIS_TOOLCHAIN=gcc
export BUILD_TYPE=release
" >%system.teamcity.build.workingDir%/custom_env.sh
sed -i "s/export REPOSITORY_URL=https:\/\/doris-thirdparty-repo.bj.bcebos.com\/thirdparty/export REPOSITORY_URL=https:\/\/doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com\/thirdparty/g" thirdparty/vars.sh

#check is there exist outdate docker,if exist, clear
docker_name=doris-p0-compile-%build.vcs.number%
set +e
outdate_docker_num=$(sudo docker ps -a --no-trunc | grep -c "$docker_name")
set -e
if [ "$outdate_docker_num" -gt 1 ]; then
    sudo docker stop $docker_name
    sudo docker rm $docker_name
fi

cd %system.teamcity.build.workingDir%
git_storage_path=$(grep storage .git/config | rev | cut -d ' ' -f 1 | rev | awk -F '/lfs' '{print $1}')
echo "sudo docker run -i --rm --name doris-clickbench-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v $HOME/.m2:/root/.m2 -v $HOME/.npm:/root/.npm -v ${git_storage_path}:/root/git -v %system.teamcity.build.workingDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c \"mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path} && cd /root/doris && export EXTRA_CXX_FLAGS=-O3 && bash build.sh --fe --be  -j 12 \"
"
sudo docker run -i --rm --name doris-clickbench-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v "$HOME"/.m2:/root/.m2 -v "$HOME"/.npm:/root/.npm -v "${git_storage_path}":/root/git -v %system.teamcity.build.workingDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c "mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path}/ && cd /root/doris && export EXTRA_CXX_FLAGS=-O3 && bash build.sh --fe --be -j 12 | tee build.log"

succ_symble="BUILD SUCCESS"
grep "$succ_symble" %system.teamcity.build.workingDir%/build.log

#check output is exist or not
if [ ! -d output ]; then
    echo -e "\e[1;31m BUILD FAIL, NO OUTPUT \e[40;37m"
    echo "clean working dir"
    cd %system.teamcity.build.workingDir%
    sudo rm -rf !(build.log)
    exit 1
fi

sudo chown -R "$USER":"$USER" output/
