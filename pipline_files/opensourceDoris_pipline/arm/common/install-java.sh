#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/

java_version='11'
if [[ -n "$1" ]]; then java_version="$1"; fi

if ! java --version; then
    cd "${pipeline_home}"

    if [[ "$java_version" == "11" ]]; then
        # java 11
        sudo yum install -y java-11-openjdk."$(uname -m)" java-11-openjdk-devel."$(uname -m)"
        java_dir=$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-11-*' | sed -n '1p')
    elif [[ "$java_version" == "8" ]]; then
        # java 8
        sudo yum install -y java-1.8.0-openjdk."$(uname -m)" java-1.8.0-openjdk-devel."$(uname -m)"
        java_dir=$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-1.8.0-*' | sed -n '1p')
    else
        echo "java version only in [8, 11]"
        exit 1
    fi

    echo "
export JAVA_HOME=$java_dir
export PATH=\$JAVA_HOME/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x

    cd -
fi

java --version
