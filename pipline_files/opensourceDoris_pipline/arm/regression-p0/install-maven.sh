#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

maven_download_url='https://dlcdn.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz'
update_maven_mirror=false

pipeline_home=${HOME}/teamcity/

if ! which mvn; then
    cd "${pipeline_home}"

    wget $maven_download_url
    file_name=$(basename $maven_download_url)
    tar -zxvf "$file_name"

    maven_dir="$pipeline_home/apache-maven-3.6.3"

    if $update_maven_mirror; then
        maven_settings_xml_path="$maven_dir"/conf/settings.xml
        row_num=$(sed -n -e "/<mirrors>/=" "$maven_settings_xml_path")
        sed -i "${row_num}a <mirror>" "$maven_settings_xml_path"
        sed -i "$((row_num + 1))a <id>aliyunmaven<\/id>" "$maven_settings_xml_path"
        sed -i "$((row_num + 2))a <mirrorOf>*<\/mirrorOf>" "$maven_settings_xml_path"
        sed -i "$((row_num + 3))a <name>aliyun maven<\/name>" "$maven_settings_xml_path"
        sed -i "$((row_num + 4))a <url>https://maven.aliyun.com/repository/public<\/url>" "$maven_settings_xml_path"
        sed -i "$((row_num + 5))a <\/mirror>" "$maven_settings_xml_path"
    fi

    echo "
export MAVEN_HOME=$maven_dir
export PATH=\$MAVEN_HOME/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x
fi

mvn --version
