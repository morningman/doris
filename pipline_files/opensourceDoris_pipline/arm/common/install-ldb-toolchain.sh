#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/

ldbtoolchain_download_url='https://github.com/amosbird/ldb_toolchain_gen/releases/download/v0.9.1/ldb_toolchain_gen.aarch64.sh'
ldbtoolchain_download_url='https://doris-pipline-1308700295.cos.ap-hongkong.myqcloud.com/teamcity/ldb_toolchain_gen.aarch64.sh'
ldbtoolchain_download_url='https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/teamcity/ldb_toolchain_gen.aarch64.sh'

# use cmake and lldb to judge if ldb toolchain installed
if ! (cmake --version &&
    lldb --version &&
    grep ldb_toolchain ~/.bashrc); then
    cd "${pipeline_home}"

    wget $ldbtoolchain_download_url
    dir_name="$pipeline_home/ldb_toolchain"
    bash ldb_toolchain_gen.aarch64.sh "$dir_name"

    echo "
export PATH=$dir_name/bin:\$PATH" >>~/.bashrc

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc
    set -x
fi

cmake --version && lldb --version
