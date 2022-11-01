#!/bin/bash
###################################################################
# this work well on HuaWeiYun Linux VM-0-12-centos 5.4.119-19-0008 #1 SMP Fri Oct 22 10:43:49 CST 2021 aarch64 aarch64 aarch64 GNU/Linux
###################################################################
set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
# teamcity_build_checkoutDir='/root/teamcity/teamcity-agent-1/work/e0bc234628561cad'
teamcity_agent_work_dir=%teamcity.agent.work.dir%

pipeline_home=${HOME}/teamcity/
skip_pipeline=${skip_pipeline:="false"}
username=${github_username:=hello-stephen}
password=${github_password:=fake-password}
# password=${github_password:=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}

echo '####check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then echo "skip build pipline" && exit 0; fi

echo '####update scripts from git@github.com:selectdb/selectdb-qa.git'
if ! which git; then sudo yum install -y git; fi
cd "${pipeline_home}"
if [[ ! -d "${pipeline_home}/selectdb-qa" ]]; then
    if ! git clone git@github.com:selectdb/selectdb-qa.git; then
        git clone https://${username}:${password}@github.com/selectdb/selectdb-qa.git
    fi
fi
qa_home="${pipeline_home}/selectdb-qa"
cd "${qa_home}" && git stash && git checkout main && git pull && cd -

#TODO maybe same file name, better to add pr id as suffix
echo '####cp scripts to checkout dir'
cd "$teamcity_build_checkoutDir"
cp -r \
    "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/common/* \
    "$teamcity_build_checkoutDir"/
cp -r \
    "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/arm/common/* \
    "$teamcity_build_checkoutDir"/
cp -r \
    "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/arm/regression-p0/* \
    "$teamcity_build_checkoutDir"/

echo "####check if utils ready, include: 
byacc patch automake libtool make which file 
ncurses-devel gettext-devel unzip bzip2 bison 
zip util-linux wget python curl jq java mysql ccache"
if ! which python; then sudo yum install python36 -y && sudo ln -s /usr/bin/python3 /usr/bin/python; fi
if ! which byacc; then sudo yum install byacc -y; fi
if ! which bison; then sudo yum install bison -y; fi
if ! which patch; then sudo yum install patch -y; fi
if ! which automake; then sudo yum install automake -y; fi
if ! which libtool; then sudo yum install libtool -y; fi
if ! which make; then sudo yum install make -y; fi
if ! which file; then sudo yum install file -y; fi
if ! (which ncurses6-config && which ncursesw6-config); then sudo yum install ncurses-devel -y; fi
if ! (which gettext && which gettextize); then sudo yum install gettext-devel -y; fi
if ! which unzip; then sudo yum install unzip -y; fi
if ! which bzip2; then sudo yum install bzip2 -y; fi
if ! which zip; then sudo yum install zip -y; fi
sudo yum install util-linux -y
bash install-java.sh
bash install-maven.sh
bash install-nodejs.sh
bash install-ldb-toolchain.sh
if [[ ! -f /etc/ssl/certs/ca-certificates.crt ]]; then
    sudo ln -s /etc/ssl/certs/ca-bundle.crt /etc/ssl/certs/ca-certificates.crt
fi
if ! which jq; then sudo yum install jq -y; fi
if ! which mysql; then sudo yum install -y mysql; fi
set +e
bash "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/benchmark/clickbench/install-ccache.sh
set -e

#TODO why .old dir still exists after set clean rules in Build Features ? wrong clean rules?
echo "####remove old checkout"
sudo rm -rf "${teamcity_agent_work_dir}"/.old/*

echo "####prepare DONE."
