#!/bin/bash
###################################################################
# Amazon EC2
# Amazon Machine Image(AMI) Description:
#     Amazon Linux 2 Kernel 5.10 AMI 2.0.20220912.1 x86_64 HVM gp2
###################################################################
set -e

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
teamcity_agent_work_dir=%teamcity.agent.work.dir%

pipeline_home=${HOME}/teamcity/
skip_pipeline=${skip_pipeline:=false}
username=${github_username:=hello-stephen}
password=${github_password:=fake-password}

echo '####check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then
    echo "skip build pipline"
    exit 0
else
    echo "no skip"
fi

if ! which git; then sudo yum install -y git; fi

echo '####update scripts from git@github.com:selectdb/selectdb-qa.git'
cd "${pipeline_home}"
if [[ ! -d "${pipeline_home}/selectdb-qa" ]]; then
    if ! git clone git@github.com:selectdb/selectdb-qa.git; then
        git clone https://${username}:${password}@github.com/selectdb/selectdb-qa.git
    fi
fi
qa_home="${pipeline_home}/selectdb-qa"
cd "${qa_home}" && git stash && git checkout main && git pull && cd -

#TODO maybe same file name, prefer to add pr id as suffix
echo '####cp scripts to checkout dir'
cp -r \
    "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/common/* \
    "$teamcity_build_checkoutDir"/
cp -r \
    "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/benchmark/clickbench/* \
    "$teamcity_build_checkoutDir"/

#############
echo '####check if utils ready, include: curl jq java mysql coscli ccache'
if [[ ! -f /etc/ssl/certs/ca-certificates.crt ]]; then
    sudo ln -s /etc/ssl/certs/ca-bundle.crt /etc/ssl/certs/ca-certificates.crt
fi
if ! jq --version; then sudo yum install jq -y; fi
if ! sudo docker --version; then
    sudo yum install -y docker
    sudo systemctl start docker
fi
if ! java --version; then
    sudo yum install -y java-11-amazon-corretto.x86_64
    export JAVA_HOME="/usr/lib/jvm/java-11-openjdk/"
    export PATH=$JAVA_HOME/bin:$PATH
fi
if ! mysql --version; then sudo yum install -y mysql; fi
bash "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/benchmark/clickbench/install-coscli.sh
set +e
bash "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/benchmark/clickbench/install-ccache.sh
set -e

#TODO why .old dir still exists after set clean rules in Build Features ? wrong clean rules?
echo "####remove old checkout"
sudo rm -rf "${teamcity_agent_work_dir}"/.old/*

set +e
bash "$teamcity_build_checkoutDir"/kill-doris-cluster.sh
set -e

echo "####prepare DONE."
