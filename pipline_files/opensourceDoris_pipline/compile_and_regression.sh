source /home/work/.bashrc
if [[ %teamcity.build.branch% == "refs/heads/master" ]];then
    echo "master no need run pipline"
    exit 0
fi
if [[ %skip_pipline% == "true" ]];then
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
outdate_builds_of_pr=(`grep ${test_branch}_${source_branch}_${target_branch}_incubator-doris $work_file/OpenSourceDorisBuild.log |awk '{print $1}'`)
for old_build_id in ${outdate_builds_of_pr[@]}
do
    echo "STRAT checking build $old_build_id"
    old_build_status=$(sh teamcity_api.sh --show_build_state $old_build_id)
    if [[ $old_build_status == "running" ]];then
        sh teamcity_api.sh --cancel_running_build $old_build_id
    fi
done

#fetch_branch=%teamcity.build.branch%/head
#commit_id=%build.vcs.number%
#gitcmd="timeout 180 git fetch https://github.com/apache/incubator-doris.git $fetch_branch && timeout 180 git checkout FETCH_HEAD && timeout 180 git checkout $commit_id"
#eval $gitcmd

#skip build which trigered by file on docs/fs_broker
#sh check_change_file.sh
#if [[ $? == 0 ]];then
#    exit 0
#fi

echo "FINISH check!"
echo
#recoding itself build info
echo "$build_id ${test_branch}_${source_branch}_${target_branch}_incubator-doris" >> $work_file/OpenSourceDorisBuild.log

res=$(git branch)
echo $res

#modify third party orgin
#sed -i "s/export REPOSITORY_URL=https:\/\/doris-thirdparty-repo.bj.bcebos.com\/thirdparty/export REPOSITORY_URL=https:\/\/doris-thirdparty.obs.ap-southeast-1.myhuaweicloud.com\/thirdparty/g" thirdparty/vars.sh
sed -i "s/export REPOSITORY_URL=https:\/\/doris-thirdparty-repo.bj.bcebos.com\/thirdparty/export REPOSITORY_URL=https:\/\/doris-thirdparty-hk-1308700295.cos.ap-hongkong.myqcloud.com\/thirdparty/g" thirdparty/vars.sh

#compile output
echo "docker run -i --rm --name doris-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %system.teamcity.build.workingDir%:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c \"cd /root/doris && sh build.sh\"
"
docker run -i --rm --name doris-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %system.teamcity.build.workingDir%:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c "cd /root/doris && sh build.sh | tee build.log"

succ_symble="BUILD SUCCESS"
grep "$succ_symble" %system.teamcity.build.workingDir%/build.log
if [ $? != 0 ];then
    echo "oops!, some fail occur, let's retry"
    pattern="npm ERR! code ELIFECYCLE"
    pr_compile_path=%system.teamcity.build.workingDir%
    res=`grep $pattern $pr_compile_path/build.log|wc -l`
    if [ $res -gt 0 ];then
        docker run -i --rm --name doris-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %system.teamcity.build.workingDir%:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c "cd /root/doris/ui && rm -rf package-lock.json && rm -rf node_modules && npm cache clean --force && cd /root/doris && echo RETRY COMPILE >> build.log  && sh build.sh|tee build.log"
    else
        docker run -i --rm --name doris-compile-%build.vcs.number% -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %system.teamcity.build.workingDir%:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c "cd /root/doris && echo RETRY COMPILE >> build.log && sh build.sh | tee build.log"
    fi
fi        
 

#check output is exist or not
if [ ! -d output ];then
    echo -e "\e[1;31m BUILD FAIL, NO OUTPUT \e[40;37m"
    exit -1
fi

#####################repare regression env##############
#find an availble cluster
cluster_center=/home/work/pipline/OpenSourceDoris
case_center=${cluster_center}/clusterRegressionCenter
work_path=${cluster_center}/clusterConf
clusters=(`find $work_path -name "Cluster*"`)
cluster_name=''
while true
do
    for cluster in ${clusters[@]}
    do
        cluster_name=`echo $cluster|rev |cut -d / -f 1|rev`
        if [[ -f ${work_path}/.${cluster_name} ]];then
            echo "$cluster_name in use, skip"
        else
            echo "Get an availbe env"
            touch ${work_path}/.${cluster_name}
            echo $cluster_name
            break
        fi
    done
    if [[ "check"$cluster_name != "check" ]];then
        break
    else
        sleep 10
    fi
done

##############prepare output to install path#############
rm -rf $work_path/$cluster_name/output
cp -r %teamcity.build.checkoutDir%/output $work_path/$cluster_name
#####################prepare regression case#############
rm -rf ${case_center}/${cluster_name}/regression-test
rm -rf ${case_center}/${cluster_name}/run-regression-test.sh
cp -r %teamcity.build.checkoutDir%/regression-test ${case_center}/${cluster_name}/
cp -r $work_path/$cluster_name/conf/regression-conf.groovy ${case_center}/${cluster_name}/regression-test/conf/
cp -r %teamcity.build.checkoutDir%/run-regression-test.sh ${case_center}/${cluster_name}/
#####################clear checkout dir##################
rm -rf %teamcity.build.checkoutDir%/*

#stop cluster
cd $work_path && sh stop_cluster.sh $cluster_name
#clear cluster
cd $work_path && sh clear_cluster.sh $cluster_name
#install cluster
cd $work_path && sh deploy_cluster.sh $cluster_name
#sleep 2min,wait for cluster ready
sleep 300
#####################run regression cases###############
cd ${case_center}/${cluster_name}
rm -rf ${case_center}/${cluster_name}/output
JAVA_OPTS="-Dteamcity.enableStdErr=${enableStdErr}" ./run-regression-test.sh --teamcity --clean --run
#delete syble file
rm ${work_path}/.${cluster_name}
