source /home/work/.bashrc
#FE_WORK_PATH=/home/work/teamcity/TeamCity/piplineWork/feUt
#mkdir $FE_WORK_PATH/%build.vcs.number.1%
#cd $FE_WORK_PATH%build.vcs.number.1%
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

work_file=/home/work/pipline/doris-test
cp -r $work_file/pipline_files/common/* ./
#skip build which trigered by file on be/docs/fs_broker
sh check_change_file.sh --is_modify_only_invoved_be %teamcity.pullRequest.number%
if [[ $? == 0 ]]; then
    exit 0
fi

#check is there exist outdate docker,if exist, clear
docker_name=doris-fe-ut-%build.vcs.number%
outdate_docker_num=$(docker ps -a --no-trunc|grep $docker_name|wc -l)
if [ $outdate_docker_num -gt 1 ];then
    docker stop $docker_name
    docker rm $docker_name
fi

exit_flag=0
ts=$(date "+%s")
echo "START RUN FE UT"
echo "docker run -i --rm --name doris-fe-ut-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %teamcity.build.checkoutDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c \"export FE_UT_PARALLEL=1 && cd /root/doris && sh run-fe-ut.sh\""
docker run -i --rm --name doris-fe-ut-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %teamcity.build.checkoutDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c "export FE_UT_PARALLEL=1 && cd /root/doris && sh run-fe-ut.sh"
if [ $? -ne 0 ]; then
    echo "UT FAILED, PLZ CHECK!"
    exit_flag=-1
fi

#clear file
echo "clear checkout dir"
cd %teamcity.build.checkoutDir%
ls | grep -v fe | xargs rm -rf {} \;

exit $exit_flag
