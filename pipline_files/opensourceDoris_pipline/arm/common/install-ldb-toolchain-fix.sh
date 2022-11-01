#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/
ldb_toolchain_dir_name="ldb_toolchain_fix"
ldb_toolchain_dir_path="$pipeline_home/$ldb_toolchain_dir_name"
teamcity_build_checkoutDir="${teamcity_build_checkoutDir:='fake-dir'}"
tmp_env_file_path="$teamcity_build_checkoutDir/.my_tmp_env"

ldb_toolchain_download_url='https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/tmp/ldy/ldb_toolchain_gen.aarch64.fix.sh'
ldb_toolchain_download_url='https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/teamcity/ldb_toolchain_gen.aarch64.fix.sh'

if [[ "$teamcity_build_checkoutDir" == "fake_dir" ]]; then echo "Error: teamcity_build_checkoutDir not set..." && exit 1; fi

if [[ -d "$ldb_toolchain_dir_path" ]]; then
    echo "
export PATH=$ldb_toolchain_dir_path/bin:\$PATH" >"$tmp_env_file_path"
    source "$tmp_env_file_path"
fi

if ! (which gdb | grep "$ldb_toolchain_dir_name"); then
    cd "${pipeline_home}"

    if ! wget $ldb_toolchain_download_url; then echo "Failed to download from $ldb_toolchain_download_url" && exit 1; fi
    ldb_toolchain_dir_path="$pipeline_home/$ldb_toolchain_dir_name"
    bash ldb_toolchain_gen.aarch64.fix.sh "$ldb_toolchain_dir_path"

    echo "
export PATH=$ldb_toolchain_dir_path/bin:\$PATH" >"$tmp_env_file_path"
    source "$tmp_env_file_path"
fi

gdb --version && cmake --version
