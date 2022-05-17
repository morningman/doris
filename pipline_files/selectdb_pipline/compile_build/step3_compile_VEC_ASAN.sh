#!/bin/bash
#echo `pwd`
export PATH=/var/local/ldb_toolchain/bin:${PATH}
git checkout %test_branch%
rm -rf vec_asan_output
echo "generate custom_env.sh"
echo "export DORIS_TOOLCHAIN=gcc" >custom_env.sh
echo "export BUILD_TYPE=ASAN" >>custom_env.sh
if [ -f custom_env.sh ];then
    echo "generate custom env succ"
else
    echo "generate custom env fail, plz check"
    exit 1
fi
bash build.sh --clean
bash build.sh

if [ -d output ];then
    mv output vec_asan_output
else
    echo "compile ASAN fail, plz check"
    exit 1
fi
