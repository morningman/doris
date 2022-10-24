#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/

maven_download_url='https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz'

if ! which mvn; then
    cd "${pipeline_home}"

    wget $maven_download_url
    file_name=$(basename $maven_download_url)
    tar -zxvf "$file_name"

    echo "
export MAVEN_HOME=$pipeline_home/apache-maven-3.6.3
export PATH=\$MAVEN_HOME/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x
fi

mvn --version
