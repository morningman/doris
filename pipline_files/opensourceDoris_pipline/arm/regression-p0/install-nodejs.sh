#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/

nodejs_download_url='https://doris-thirdparty-repo.bj.bcebos.com/thirdparty/node-v16.3.0-linux-arm64.tar.xz'

if ! which node; then
    cd "${pipeline_home}"

    wget $nodejs_download_url
    file_name=$(basename $nodejs_download_url)
    tar -xvf "$file_name"

    echo "
export NODEJS_HOME=$pipeline_home/node-v16.3.0-linux-arm64
export PATH=\$NODEJS_HOME/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x
fi

node --version
