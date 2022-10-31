#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/

autoconf_download_url='http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz'

if ! which autoconf; then
    cd "${pipeline_home}"

    wget $autoconf_download_url
    file_name=$(basename $autoconf_download_url)
    tar -zxvf "$file_name"
    cd 'autoconf-2.69'
    ./configure && make

    echo "
export PATH=$pipeline_home/autoconf-2.69/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x
fi

autoconf --version
