#!/bin/bash
set -ex

pipeline_home=${HOME}/teamcity/
checkout_dir=%teamcity.build.checkoutDir%
skip_pipeline=${skip_pipeline:=false}

echo 'check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then
    echo "skip build pipline"
    exit 0
else
    echo "no skip"
fi

echo '====prepare===='

echo 'update scripts from git@github.com:selectdb/selectdb-qa.git'
cd "${pipeline_home}"
if [[ ! -d "${pipeline_home}/selectdb-qa" ]]; then
    git clone git@github.com:selectdb/selectdb-qa.git
fi
qa_home="${pipeline_home}/selectdb-qa"
cd "${qa_home}" && git stash && git checkout main && git pull && cd -

#TODO maybe same file name, prefer to add pr id as suffix
cp -r \
    "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/common/* \
    "$checkout_dir"/
cp -r \
    "${pipeline_home}"/selectdb-qa/pipline_files/opensourceDoris_pipline/benchmark/clickbench/*.sh \
    "$checkout_dir"/

#############
if [[ ! -f /etc/ssl/certs/ca-certificates.crt ]]; then
    sudo ln -s /etc/ssl/certs/ca-bundle.crt /etc/ssl/certs/ca-certificates.crt
fi
if ! jp --version; then sudo yum install jq -y; fi
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

sudo rm -rf %teamcity.agent.work.dir%/.old/*

set +e
bash kill-doris-cluster.sh
set -e
