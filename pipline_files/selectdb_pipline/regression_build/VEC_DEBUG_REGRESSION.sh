#!/bin/bash
###########check regression case running or not#########
res=`ps -ef|grep run-regression-test.sh|grep -v grep|awk '{print $2}'|wc -l`
if [ $res -gt 0 ];then
    res=$(pwdx `ps -ef|grep run-regression-test.sh|grep -v grep|awk '{print $2}'`|grep %clusterName%)
    if [ "check""$res" != "check" ];then
        echo %clusterName%" is running regression, please stop it!"
        exit -1
    fi
fi
#####################check fe/be version################
#####################repare regression env##############
cluster_name=%clusterName%
cluster_conf_path=%clusterConfPath%
case_center=%caseCenter%

echo $cluster_name
echo $cluster_conf_path
echo $case_center
#sleep 600
#cp output to clusterConf
######debug#####
if [ "_%skip_update_bin%" = "_false" ];then
    rm -rf $cluster_conf_path/$cluster_name/output
    cluster_output=`echo ${cluster_name}| tr 'A-Z' 'a-z'`
    cp -r /home/zcp/selectdb/${cluster_output}_output $cluster_conf_path/$cluster_name/output
    #cluster_name_in_cos=`echo ${cluster_name}| tr 'A-Z' 'a-z'`
    #coscmd download regression_compile/${cluster_name_in_cos}_output.tar.gz $cluster_conf_path/$cluster_name/output.tar.gz
    #cd $cluster_conf_path/$cluster_name
    #tar -zxvf output.tar.gz output
    #rm -rf $cluster_conf_path/$cluster_name/output.tar.gz
    
    #stop cluster
    #cd $cluster_conf_path && bash stop_cluster.sh $cluster_name
    #clear cluster
    #cd $cluster_conf_path && bash clear_cluster.sh $cluster_name
    #install cluster
    cd $cluster_conf_path && bash deploy_cluster.sh $cluster_name
    if [ "_%openVec%" = "_true" ];then
        cd $cluster_conf_path && bash open_cluster_vec.sh $cluster_name
    fi
else
    echo "skip update bin"
fi
#######debug#####
#####################repare regression case#############
if [ "_%skip_update_cases%" = "_false" ];then
    rm -rf ${case_center}/${cluster_name}/regression-test
    rm -rf ${case_center}/${cluster_name}/regression-test.tar.gz
    rm -rf ${case_center}/${cluster_name}/run-regression-test.sh
    #coscmd download regression_compile/regression-test.tar.gz ${case_center}/${cluster_name}/regression-test.tar.gz
    #cd ${case_center}/${cluster_name}/
    #tar -zxvf regression-test.tar.gz
    #cd -
    #coscmd download regression_compile/run-regression-test.sh ${case_center}/${cluster_name}/run-regression-test.sh
    cp -r /home/zcp/selectdb/regression-test ${case_center}/${cluster_name}/
    cp -r /home/zcp/selectdb/run-regression-test.sh ${case_center}/${cluster_name}/
    chmod 775 ${case_center}/${cluster_name}/run-regression-test.sh
    cp -r ${cluster_conf_path}/$cluster_name/conf/regression-conf.groovy ${case_center}/${cluster_name}/regression-test/conf
    ###sleep 300
else
    echo "skip update bin"
fi
######################modify case conf##################
#object conf
res=`grep s3Endpoint ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy|wc -l`
if [ "check"$res != "check" ];then
    echo "s3Endpoint already exists";
else
    echo "" ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy
    echo "s3Endpoint = \"%s3Endpoint%\"" >> ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy
fi
res=`grep s3BucketName ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy|wc -l`
if [ "check"$res != "check" ];then
    echo "s3BucketName already exists";
else
    echo "s3BucketName = \"%s3BucketName%\"" >> ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy
fi
res=`grep ak ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy|wc -l`
if [ "check"$res != "check" ];then
    echo "ak already exists";
else
    echo "ak = \"%ak%\"" >> ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy
fi
res=`grep sk ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy|wc -l`
if [ "check"$res != "check" ];then
    echo "sk already exists";
else
    echo "sk = \"%sk%\"" >> ${case_center}/${cluster_name}/regression-test/conf/regression-conf.groovy
fi

#####################run regression cases###############
echo ${case_center}/${cluster_name}
cd ${case_center}/${cluster_name}
#rm -rf ${case_center}/${cluster_name}/output
export LANG=C.UTF-8
echo "JAVA_OPTS=\"-Dteamcity.enableStdErr=${enableStdErr}\" ./run-regression-test.sh --clean --teamcity --run -times %regression_times%"
JAVA_OPTS="-Dteamcity.enableStdErr=${enableStdErr}" ./run-regression-test.sh --clean --teamcity --run -times %regression_times% -parallel 10 -suiteParallel 10
echo "DONE"
