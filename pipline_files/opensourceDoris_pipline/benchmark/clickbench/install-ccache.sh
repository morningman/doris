#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/
ccache_download_url=https://github.com/ccache/ccache/releases/download/v4.7/ccache-4.7-linux-x86_64.tar.xz

if ccache --version; then
  ccache -s
  exit
fi

if ! sudo yum install -y ccache; then
  ROOT=$(pwd)
  cd "${pipeline_home}"
  wget "$ccache_download_url"
  file_name="$(basename ${ccache_download_url})"
  tar -xf "$file_name"
  dir_name="${file_name/.tar.xz/}"
  echo "
export PATH=$ROOT/$dir_name:\$PATH" >>~/.bashrc
  # shellcheck source=/dev/null
  source ~/.bashrc
fi
ccache --version

# echo "
# export CCACHE_DIR=$(pwd)/.ccache
# " >>~/.bashrc

# shellcheck source=/dev/null
source ~/.bashrc

ccache -M 100G
ccache -s
