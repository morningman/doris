#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc
set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
# teamcity_build_checkoutDir='/root/doris'

DORIS_HOME="$teamcity_build_checkoutDir/output/"
teamcity_home=${HOME}/teamcity/
cacheDataPath="$teamcity_home/data/regression"
if [[ ! -d "$cacheDataPath" ]]; then mkdir -p "$cacheDataPath"; fi
s3Region=${s3Region:='ap-hongkong'}
s3Endpoint=${s3Endpoint:='cos.ap-hongkong.myqcloud.com'}
s3BucketName=${s3BucketName:='doris-build-hk-1308700295'}
ak=${ak:='fake-ak'}
sk=${sk:='fake-sk'}

echo "####edit regression conf"
fe_conf_query_port=$(grep query_port "$DORIS_HOME"/fe/conf/fe_custom.conf | sed 's/ //g' | cut -d'=' -f2)
port_tail=${fe_conf_query_port:0-1}
regression_conf_path="$teamcity_build_checkoutDir/regression-test/conf/regression-conf.groovy"
sed -i "s/8030/803$port_tail/g;
s/9010/901$port_tail/g;
s/9020/902$port_tail/g;
s/9030/903$port_tail/g;
s/8040/804$port_tail/g;
s/8060/806$port_tail/g;
s/9040/904$port_tail/g;
s/9060/906$port_tail/g;" "$regression_conf_path"
echo "
s3Region = $s3Region
s3Endpoint = $s3Endpoint
s3BucketName = $s3BucketName
ak = $ak
sk = $sk
cacheDataPath = '$cacheDataPath'
enableBrokerLoad=false
" >>"$regression_conf_path"
# sf1DataPath = "https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/regression"
# sf1DataPath = "https://doris-build-hk-1308700295.cos.ap-hongkong.myqcloud.com/regression"

echo "####run regression test"
if bash run-regression-test.sh --teamcity --clean --run -parallel 3 -suiteParallel 3 -actionParallel 3 -g p0; then
    # green
    echo -e "
\033[32m
##########################
run regression test, PASS
##########################
\033[0m"
    exit 0
else
    # red
    echo -e "
\033[31m
##########################
run regression test, FAIL
##########################
\033[0m"
    exit 1
fi
