source /home/work/.bashrc
#set ccache size
if [ $(df -h | grep ccache | wc -l) -eq 1 ]; then
    echo "exist ccache disk"
    ccache_disk_size=$(df -h | grep ccache | awk '{print $2}' | cut -d 'G' -f 1)
    ccahce_size=$(expr $ccache_disk_size - 5)
    ccache -M ${ccahce_size}G
fi
echo "clean old checkout dir which was last changed 3 days before"
curdir=%teamcity.build.checkoutDir%
basedir=$(dirname $curdir)
cd $basedir
ls | find -ctime +3 -maxdepth 1 | xargs rm -rf
cd -

if [[ %teamcity.build.branch% == "refs/heads/master" ]]; then
    echo "master no need run pipline"
    exit 0
fi
if [[ %skip_pipline% == "true" ]]; then
    echo "skip build pipline"
    exit 0
fi

#generate download url
work_file=/home/work/pipline/doris-test
cp -r $work_file/pipline_files/common/* ./
#skip build which trigered by file on fe/docs/fs_broker
bash check_change_file.sh --is_modify_only_invoved_fe %teamcity.pullRequest.number% 2>/dev/null
if [[ $? == 0 ]]; then
    exit 0
fi

#check is there exist outdate docker,if exist, clear
docker_name=doris-be-ut-%build.vcs.number%
outdate_docker_num=$(docker ps -a --no-trunc | grep $docker_name | wc -l)
if [ $outdate_docker_num -gt 1 ]; then
    docker stop $docker_name
    docker rm $docker_name
fi

#get git storage path, mount that to docker
cd %system.teamcity.build.workingDir%
git_storage_path=$(grep storage .git/config | rev | cut -d ' ' -f 1 | rev | awk -F '/lfs' '{print $1}')

exit_flag=0
ts=$(date "+%s")
echo "START BUILD AND RUN BE UT"
echo "docker run -i --rm --name doris-be-ut-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v /mnt/ccache/.ccache:/root/.ccache -v ${git_storage_path}:/root/git -v %teamcity.build.checkoutDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c \"mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path} && export CCACHE_LOGFILE=/tmp/cache.debug && cd /root/doris && bash run-be-ut.sh --clean --run -j 12\""
docker run -i --rm --name doris-be-ut-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v /mnt/ccache/.ccache:/root/.ccache -v ${git_storage_path}:/root/git -v %teamcity.build.checkoutDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c "mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path} && export CCACHE_LOGFILE=/tmp/cache.debug && cd /root/doris && bash run-be-ut.sh --clean --run -j 5"
if [ $? -ne 0 ]; then
    echo "UT FAILED, PLZ CHECK!"
    exit_flag=-1
fi

if [ ! -d %system.teamcity.build.checkoutDir%/be/ut_build_ASAN/gtest_output ]; then
    echo "UT FAILED, PLZ CHECK!"
    exit_flag=-1
fi

###clear checkout dir
echo "clear checkout dir"
cd %teamcity.build.checkoutDir%
ls | grep -v be | xargs rm -rf {} \;
if [ -d %system.teamcity.build.checkoutDir%/be/ut_build_ASAN ]; then
    cd %teamcity.build.checkoutDir%/be/ut_build_ASAN
    ls | grep -v gtest_output | xargs rm -rf {} \;
fi

exit $exit_flag
