#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/

if ! java --version; then
    cd "${pipeline_home}"

    # java 11
    sudo yum install -y java-11-openjdk.aarch64 java-11-openjdk-devel.aarch64
    java_dir=$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-11-*' | sed -n '1p')

    # java 8
    # sudo yum install -y java-1.8.0-openjdk.aarch64 java-1.8.0-openjdk-devel.aarch64
    # java_dir=$(find /usr/lib/jvm -maxdepth 1 -type d -name 'java-1.8.0-*' | sed -n '1p')

    echo "
export JAVA_HOME=$java_dir
export PATH=\$JAVA_HOME/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x
fi

java --version
