source /home/work/.bashrc
if [[ %teamcity.build.branch% == "refs/heads/master" ]];then
    echo "master no need run pipline"
    exit 0
fi
if [[ %skip_pipline% == "true" ]];then
    echo "skip build pipline"
    exit 0
fi

#generate download url
work_file=/home/work/pipline/doris-test
cp -r $work_file/pipline_files/common/* ./
#skip build which trigered by file on docs/fs_broker
sh check_change_file.sh
if [[ $? == 0 ]];then
    exit 0
fi

echo "START BUILD AND RUN BE UT"
echo "docker run -i --rm --name doris-be-ut-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %teamcity.build.checkoutDir%:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c \"cd /root/doris && sh run-be-ut.sh --clean --run -j 12\""
docker run -i --rm --name doris-be-ut-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %teamcity.build.checkoutDir%:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c "cd /root/doris && sh run-be-ut.sh --clean --run -j 12"
if [ $? -ne 0 ];then
    echo "UT FAILED, PLZ CHECK!"
    exit -1
fi

if [ ! -d %system.teamcity.build.checkoutDir%/be/ut_build_ASAN/gtest_output ];then
    echo "UT FAILED, PLZ CHECK!"
    exit -1
fi

###clear checkout dir
cd %teamcity.build.checkoutDir%
ls |grep -v be|xargs rm -rf {} \;
cd %teamcity.build.checkoutDir%/be/ut_build_ASAN
ls |grep -v gtest_output|xargs rm -rf {} \;
