#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex
source ~/.bashrc_tmp

pipeline_home=${HOME}/teamcity/
ldb_toolchain_dir_name="ldb_toolchain_fix"

ldb_toolchain_download_url='https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/tmp/ldy/ldb_toolchain_gen.aarch64.fix.sh'
ldb_toolchain_download_url='https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/teamcity/ldb_toolchain_gen.aarch64.fix.sh'

if ! (which cmake | grep "$ldb_toolchain_dir_name"); then
    cd "${pipeline_home}"

    if ! wget $ldb_toolchain_download_url; then echo "Failed to download from $ldb_toolchain_download_url" && exit 1; fi
    ldb_toolchain_dir_path="$pipeline_home/$ldb_toolchain_dir_name"
    bash ldb_toolchain_gen.aarch64.fix.sh "$ldb_toolchain_dir_path"

    echo "
export PATH=$ldb_toolchain_dir_path/bin:\$PATH" >>~/.bashrc_tmp

    set +x
    # shellcheck source=/dev/null
    source ~/.bashrc_tmp
    set -x
fi

cmake --version && gdb --version
