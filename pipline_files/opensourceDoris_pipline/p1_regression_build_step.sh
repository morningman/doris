#!/bin/bash
source /home/work/.bashrc
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

if [[ "%teamcity.build.branch%" == "refs/heads/master" ]]; then
    echo "master no need run pipline"
    exit 0
fi
if [[ "%skip_pipline%" == "true" ]]; then
    echo "skip build pipline"
    exit 0
fi

#recoding para
pipline_path=/home/work/pipline/
work_file=/home/work/pipline/doris-test
cp -r $work_file/pipline_files/common/* ./
pullrequestID=%teamcity.pullRequest.number%
test_branch=$pullrequestID
source_branch=%teamcity.pullRequest.source.branch%
target_branch=%teamcity.pullRequest.target.branch%
build_id=%teamcity.build.id%

cp -r $work_file/pipline_files/common/* ./
##kill older commits of pull request
outdate_builds_of_pr=($(grep ${test_branch}_${source_branch}_${target_branch}_incubator-doris $work_file/OpenSourceDorisBuild.log | awk '{print $1}'))
for old_build_id in ${outdate_builds_of_pr[@]}; do
    echo "STRAT checking build $old_build_id"
    old_build_status=$(bash teamcity_api.sh --show_build_state $old_build_id)
    if [[ $old_build_status == "running" ]]; then
        bash teamcity_api.sh --cancel_running_build $old_build_id
    fi
done

#fetch_branch=%teamcity.build.branch%/head
#commit_id=%build.vcs.number%
#gitcmd="timeout 180 git fetch https://github.com/apache/doris.git $fetch_branch && timeout 180 git checkout FETCH_HEAD && timeout 180 git checkout $commit_id"
#eval $gitcmd

#skip build which trigered by file under docs/zh-CN/docs/sql-manual/
bash check_change_file.sh --is_modify_only_invoved_doc %teamcity.pullRequest.number% 2>/dev/null
if [[ $? == 0 ]]; then
    exit 0
fi

echo "FINISH check!"
echo
#recoding itself build info
echo "$build_id ${test_branch}_${source_branch}_${target_branch}_doris" >>$work_file/OpenSourceDorisBuild.log

res=$(git branch)
echo $res
echo "export DORIS_TOOLCHAIN=gcc" >%system.teamcity.build.workingDir%/custom_env.sh
# echo "export BUILD_TYPE=RELEASE" >> %system.teamcity.build.workingDir%/custom_env.sh
echo "export BUILD_TYPE=ASAN" >>%system.teamcity.build.workingDir%/custom_env.sh

#modify third party orgin
#sed -i "s/export REPOSITORY_URL=https:\/\/doris-thirdparty-repo.bj.bcebos.com\/thirdparty/export REPOSITORY_URL=https:\/\/doris-thirdparty.obs.ap-southeast-1.myhuaweicloud.com\/thirdparty/g" thirdparty/vars.sh
sed -i "s/export REPOSITORY_URL=https:\/\/doris-thirdparty-repo.bj.bcebos.com\/thirdparty/export REPOSITORY_URL=https:\/\/doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com\/thirdparty/g" thirdparty/vars.sh

#check is there exist outdate docker,if exist, clear
docker_name=doris-p1-compile-%build.vcs.number%
outdate_docker_num=$(docker ps -a --no-trunc | grep $docker_name | wc -l)
if [ $outdate_docker_num -gt 1 ]; then
    docker stop $docker_name
    docker rm $docker_name
fi

#get git storage path, mount that to docker
cd %system.teamcity.build.workingDir%
git_storage_path=$(grep storage .git/config | rev | cut -d ' ' -f 1 | rev | awk -F '/lfs' '{print $1}')

echo "docker run -i --rm --name doris-p1-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v /mnt/ccache/.ccache:/root/ccache -v ${git_storage_path}:/root/git -v %system.teamcity.build.workingDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c \"mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path} && cd /root/doris && export EXTRA_CXX_FLAGS=-O3 && bash build.sh --fe --be --broker --hive-udf --spark-dpp --java-udf -j 5 \"
"
docker run -i --rm --name doris-p1-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v /mnt/ccache/.ccache:/root/.ccache -v ${git_storage_path}:/root/git -v %system.teamcity.build.workingDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c "mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path}/ && cd /root/doris && export EXTRA_CXX_FLAGS=-O3 && bash build.sh --fe --be --broker --hive-udf --spark-dpp --java-udf -j 5 | tee build.log"

succ_symble="BUILD SUCCESS"
grep "$succ_symble" %system.teamcity.build.workingDir%/build.log
if [ $? != 0 ]; then
    echo "oops!, some fail occur, let's retry"
    pattern="npm ERR! code ELIFECYCLE"
    pr_compile_path=%system.teamcity.build.workingDir%
    res=$(grep $pattern $pr_compile_path/build.log | wc -l)
    if [ $res -gt 0 ]; then
        docker run -i --rm --name doris-p1-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v /mnt/ccache/.ccache:/root/ccache -v ${git_storage_path}:/root/git -v %system.teamcity.build.workingDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c "mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path}/ && cd /root/doris/ui && rm -rf package-lock.json && rm -rf node_modules && npm cache clean --force && cd /root/doris && echo RETRY COMPILE >> build.log && export EXTRA_CXX_FLAGS=-O3 && bash build.sh --fe --be --broker --hive-udf --spark-dpp --java-udf -j 5 |tee build.log"
    else
        docker run -i --rm --name doris-p1-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v /mnt/ccache/.ccache:/root/ccache -v ${git_storage_path}:/root/git -v %system.teamcity.build.workingDir%:/root/doris apache/doris:build-env-ldb-toolchain-latest /bin/bash -c "mkdir -p ${git_storage_path} && cp -r /root/git/* ${git_storage_path}/ && cd /root/doris && echo RETRY COMPILE >> build.log && export EXTRA_CXX_FLAGS=-O3 && bash build.sh --fe --be --broker --hive-udf --spark-dpp --java-udf -j 5 | tee build.log"
    fi
fi

#check output is exist or not
if [ ! -d output ]; then
    echo -e "\e[1;31m BUILD FAIL, NO OUTPUT \e[40;37m"
    echo "clean working dir"
    cd %system.teamcity.build.workingDir%
    ls | grep -v build.log | xargs rm -rf
    exit -1
fi

#####################repare regression env##############
#find an availble cluster
cluster_center=/home/work/pipline/OpenSourceDoris
case_center=${cluster_center}/clusterRegressionCenter/P1/
work_path=${cluster_center}/clusterConf/P1/
clusters=($(find $work_path -name "Cluster*"))
cluster_name=''
while true; do
    for cluster in ${clusters[@]}; do
        tmp_cluster_name=$(echo $cluster | rev | cut -d / -f 1 | rev)

        if [[ -f ${work_path}/.${tmp_cluster_name} ]]; then
            timestamp=$(date +%s)
            filetimestamp=$(stat -c %Y ${work_path}/.${tmp_cluster_name})
            timediff=$(($timestamp - $filetimestamp))
            if [ $timediff -gt 10800 ]; then
                echo "${work_path}/.${tmp_cluster_name} create in before 3h(3*60*60s) ago, this env can be used"
                rm -rf ${work_path}/.${tmp_cluster_name}
            else
                echo "$tmp_cluster_name in use, skip"
            fi

        else
            echo "Get an availbe env"
            cluster_name=${tmp_cluster_name}
            echo "touch ${work_path}/.${cluster_name}"
            touch ${work_path}/.${cluster_name}
            echo $cluster_name
            be_port=$(grep be_port ${work_path}/$cluster_name/conf/be.conf | cut -d " " -f 3)
            bash check_and_kill_deleted_proc.sh $cluster_name $be_port "P1"
            break
        fi
    done
    if [[ "check"$cluster_name != "check" ]]; then
        break
    else
        sleep 10
    fi
done

##############prepare output to install path#############
rm -rf $work_path/$cluster_name/output
cp -r %teamcity.build.checkoutDir%/output $work_path/$cluster_name
#####################prepare regression case#############
mkdir -p ${case_center}/${cluster_name}
rm -rf ${case_center}/${cluster_name}/regression-test
rm -rf ${case_center}/${cluster_name}/run-regression-test.sh
cp -r %teamcity.build.checkoutDir%/regression-test ${case_center}/${cluster_name}/
cp -r $work_path/$cluster_name/conf/regression-conf.groovy ${case_center}/${cluster_name}/regression-test/conf/
cp -r %teamcity.build.checkoutDir%/run-regression-test.sh ${case_center}/${cluster_name}/
#####################clear checkout dir##################
rm -rf %teamcity.build.checkoutDir%/*
sysctl -w vm.max_map_count=2000000

#stop cluster
cd $work_path && bash stop_cluster.sh $cluster_name
#clear cluster
cd $work_path && bash clear_cluster.sh $cluster_name
#install cluster
cd $work_path && bash deploy_cluster.sh $cluster_name
#sleep 2min,wait for cluster ready
sleep 120
#####################check cluster status###############
install_path=$(grep "CLUSTER_DIR=" $work_path/deploy_cluster.sh | cut -d = -f 2 | cut -d $ -f 1)
install_path=${install_path}/$cluster_name
be_pid=''
if [ -f $install_path/be/bin/be.pid ]; then
    echo "get be.pid"
    be_pid=$(cat $install_path/be/bin/be.pid)
    echo "get be.pid: $be_pid"
fi

echo "---------start checking cluster status-----------"
cd $work_path && bash check_cluster_status.sh $cluster_name
if [ "_$?" != "_0" ]; then
    echo "cluster start fail, plz check!"
    cd $work_path && bash stop_cluster.sh $cluster_name

    #backup cluster log to s3
    echo "cluster start fail, backup log, conf and variables to cos"
    backup_path=/home/work/pipline/backup_center
    Backup_cluster_name=${pullrequestID}_%build.vcs.number%_p1
    rm -rf $backup_path/$Backup_cluster_name
    mkdir -p $backup_path/$Backup_cluster_name
    #grep install path
    CLUSTER=$cluster_name
    install_path=$(grep "CLUSTER_DIR=" $work_path/deploy_cluster.sh | cut -d = -f 2 | cut -d $ -f 1)
    install_path=${install_path}/$CLUSTER
    #backup fe
    mkdir $backup_path/$Backup_cluster_name/fe
    #mv $install_path/fe/bin $backup_path/$Backup_cluster_name/fe/
    echo "cp -r $install_path/fe/bin $backup_path/$Backup_cluster_name/fe/"
    cp -r $install_path/fe/conf $backup_path/$Backup_cluster_name/fe/
    echo "cp -r $install_path/fe/log $backup_path/$Backup_cluster_name/fe/"
    cp -r $install_path/fe/log $backup_path/$Backup_cluster_name/fe/
    #backup be
    mkdir $backup_path/$Backup_cluster_name/be
    #echo "mv $install_path/be/bin $backup_path/$Backup_cluster_name/be/"
    #mv $install_path/be/bin $backup_path/$Backup_cluster_name/be/
    echo "cp -r $install_path/be/conf $backup_path/$Backup_cluster_name/be/"
    cp -r $install_path/be/conf $backup_path/$Backup_cluster_name/be/
    echo "cp -r $install_path/be/log $backup_path/$Backup_cluster_name/be/"
    cp -r $install_path/be/log $backup_path/$Backup_cluster_name/be/
    cp -r $case_center/$cluster_name/output/regression-test/log/doris-regression-test.*.log $backup_path/$Backup_cluster_name/

    #echo "BACKUP DONE!"
    cd $backup_path
    tar -zcvf OpenSourcePiplineRegression_${Backup_cluster_name}.tar.gz $Backup_cluster_name
    python coscmdApi.py -o OpenSourcePiplineRegression_${Backup_cluster_name}.tar.gz -p regression
    rm -rf $backup_path/*${Backup_cluster_name}*

    #delete syble file
    rm ${work_path}/.${cluster_name}
    echo "REGRESSION CLUSTER START FAIL EXIT"
    exit 1
fi
echo "------------clsuter status ok!------------------"

#####################run regression cases###############
cd ${case_center}/${cluster_name}
rm -rf ${case_center}/${cluster_name}/output
echo "./run-regression-test.sh --teamcity --clean --run -parallel 10 -g p1"
JAVA_OPTS="-Dteamcity.enableStdErr=${enableStdErr}" ./run-regression-test.sh --teamcity --clean --run -parallel 5 -suiteParallel 2 -actionParallel 5 -g p1 -xd datev2
exit_flag=$?

if [ $exit_flag -ne 0 ]; then
    echo
    echo "some regression fail, stop regresion!"
    echo
fi

echo
echo
#after run, sleep a while to check is there exist mem leak
cd $work_path && bash stop_cluster_grace.sh $cluster_name

#if exists case failed, backup fe, be and log
echo
echo
backup_path=/home/work/pipline/backup_center
if [ -f $case_center/$cluster_name/output/regression-test/log/doris-regression-test.*.log ]; then
    fail_count=$(grep "Some suites failed" $case_center/$cluster_name/output/regression-test/log/doris-regression-test.*.log | wc -l)
    succ_count=$(grep "All suites success" $case_center/$cluster_name/output/regression-test/log/doris-regression-test.*.log | wc -l)
    if [ $succ_count -eq 0 ]; then
        echo "regression fail, backup log, conf and variables to cos"
        Backup_cluster_name=${pullrequestID}_%build.vcs.number%_p1
        rm -rf $backup_path/$Backup_cluster_name
        mkdir -p $backup_path/$Backup_cluster_name
        #grep install path
        CLUSTER=$cluster_name
        install_path=$(grep "CLUSTER_DIR=" $work_path/deploy_cluster.sh | cut -d = -f 2 | cut -d $ -f 1)
        install_path=${install_path}/$CLUSTER
        #backup fe
        #echo "BACKUP PATH: $backup_path/$Backup_cluster_name"
        mkdir $backup_path/$Backup_cluster_name/fe
        #echo "mv $install_path/fe/bin $backup_path/$Backup_cluster_name/fe/"
        #mv $install_path/fe/bin $backup_path/$Backup_cluster_name/fe/
        echo "cp -r $install_path/fe/bin $backup_path/$Backup_cluster_name/fe/"
        cp -r $install_path/fe/conf $backup_path/$Backup_cluster_name/fe/
        echo "cp -r $install_path/fe/log $backup_path/$Backup_cluster_name/fe/"
        cp -r $install_path/fe/log $backup_path/$Backup_cluster_name/fe/
        #backup be
        mkdir $backup_path/$Backup_cluster_name/be
        #echo "mv $install_path/be/bin $backup_path/$Backup_cluster_name/be/"
        #mv $install_path/be/bin $backup_path/$Backup_cluster_name/be/
        echo "cp -r $install_path/be/conf $backup_path/$Backup_cluster_name/be/"
        cp -r $install_path/be/conf $backup_path/$Backup_cluster_name/be/
        echo "cp -r $install_path/be/log $backup_path/$Backup_cluster_name/be/"
        cp -r $install_path/be/log $backup_path/$Backup_cluster_name/be/
        cp -r $case_center/$cluster_name/output/regression-test/log/doris-regression-test.*.log $backup_path/$Backup_cluster_name/

        #backup variables
        fe_ip=$(cat $work_path/$CLUSTER/fe_hosts | awk -F '@' '{print $2}')
        fe_port=$(grep query_port $work_path/$CLUSTER/conf/fe.conf | awk -F '=' '{print $2}' | sed 's/[ ]*//g')
        mysql -h $fe_ip -P$fe_port -u root -e "show variables" >$backup_path/$Backup_cluster_name/show_variables

        #check generate core or not
        #if the last 2 line is not start with start time, mean there maybe has core
        core_flag=$(tail -n 2 $install_path/be/log/be.out | grep "start time:" | wc -l)
        if [ ! -z ${be_pid} ]; then
            #be_pid=$(cat $install_path/be/bin/be.pid)
            echo "cat $install_path/be/bin/be.pid: ${be_pid}"
            centos_flag=$(uname -a | grep centos | wc -l)
            ubuntu_flag=$(uname -a | grep ubuntu | wc -l)
            if [[ "check${centos_flag}" == "check1" ]]; then
                echo "this build on centos system"
                if [ -f /var/lib/systemd/coredump/core.doris_be*.${be_pid}.* ]; then
                    echo "find /var/lib/systemd/coredump -name core.doris_be*.${be_pid}.*"
                    corename=$(find /var/lib/systemd/coredump -name core.doris_be*.${be_pid}.*)
                    echo "we got corename: $corename"
                    #wait 60 for coredump finish
                    sleep 60
                    cp -r $corename $backup_path/$Backup_cluster_name
                    mkdir -p $backup_path/$Backup_cluster_name/be/lib
                    cp -r $install_path/be/lib/doris_be $backup_path/$Backup_cluster_name/be/lib/
                else
                    echo "no core dump file"
                fi
            elif [[ "check${ubuntu_flag}" == "check1" ]]; then
                echo "this build on ubuntu system"
                ####to do: ubuntu format need fix
                if [ -f /var/lib/apport/coredump//core.*${be_pid}.* ]; then
                    echo "find /var/lib/apport/coredump/ -name core.*${be_pid}.*"
                    corename=$(find /var/lib/apport/coredump/ -name core.*${be_pid}.*)
                    echo "we got corename: $corename"
                    #wait 60 for coredump finish
                    sleep 60
                    cp -r $corename $backup_path/$Backup_cluster_name
                    mkdir -p $backup_path/$Backup_cluster_name/be/lib
                    cp -r $install_path/be/lib/doris_be $backup_path/$Backup_cluster_name/be/lib/
                else
                    echo "no core dump file"
                fi
            fi
        fi

        #echo "BACKUP DONE!"
        cd $backup_path
        tar -zcvf OpenSourcePiplineRegression_${Backup_cluster_name}.tar.gz $Backup_cluster_name
        python coscmdApi.py -o OpenSourcePiplineRegression_${Backup_cluster_name}.tar.gz -p regression

        rm -rf $backup_path/*${Backup_cluster_name}*
    else
        echo "run successful, no need backup"
    fi
else
    if [ ! -f ${case_center}/${cluster_name}/regression-test/framework/target/regression-test-1.0-SNAPSHOT.jar ]; then
        echo "maybe build regression fail. plz check"
        #exit_flag=-1
    fi
    echo "case not run successfully, no need backup"
fi

#delete syble file
echo "Start clean work:"
if [ -f ${work_path}/.${cluster_name} ]; then
    echo "clean syble file: " ${work_path}/.${cluster_name}
    rm ${work_path}/.${cluster_name}
else
    echo ${work_path}/.${cluster_name} "not exist, no need delete, but should check! "
fi

exit ${exit_flag}
