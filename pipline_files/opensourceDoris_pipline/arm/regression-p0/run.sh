#!/bin/bash

set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
# teamcity_build_checkoutDir='/root/doris'

DORIS_HOME="$teamcity_build_checkoutDir/output/"

# edit regression conf
echo "####edit regression conf"
fe_conf_query_port=$(grep query_port "$DORIS_HOME"/fe/conf/fe_custom.conf | sed 's/ //g' | cut -d'=' -f2)
port_tail=${fe_conf_query_port:0-1}
regression_conf_path="$teamcity_build_checkoutDir/regression-test/conf/regression-conf.groovy"
sed -i "s/8030/803$port_tail/g;
s/9010/901$port_tail/g;
s/9020/902$port_tail/g;
s/9030/904$port_tail/g;
s/8040/804$port_tail/g;
s/8060/806$port_tail/g;
s/9040/904$port_tail/g;
s/9060/906$port_tail/g;" "$regression_conf_path"

echo "####run regression test"
if bash run-regression-test.sh --teamcity --clean --run -parallel 3 -suiteParallel 3 -actionParallel 3 -g p0; then
    echo -e "
\033[32m
##########################
run regression test, PASS
##########################
\033[0m"
    exit 0
else
    echo -e "
\033[31m
##########################
run regression test, FAIL
##########################
\033[0m"
    exit 1
fi
