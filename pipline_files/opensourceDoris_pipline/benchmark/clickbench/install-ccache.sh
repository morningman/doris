#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

pipeline_home=${HOME}/teamcity/
ccache_download_url=https://github.com/ccache/ccache/releases/download/v4.7/ccache-4.7-linux-x86_64.tar.xz
# ccache_download_url=https://github.com/ccache/ccache/releases/download/v3.7.7/ccache-3.7.7.tar.xz

if ccache --version; then
  # -v for version 4.7
  ccache -sv 2>>/dev/null || ccache -s
  exit
fi

ROOT=$(pwd)
if sudo yum install -y ccache; then
  echo "
export CCACHE_DIR=$HOME/.ccache" >>~/.bashrc
elif sudo apt install -y ccache; then
  echo "
export CCACHE_DIR=$HOME/.ccache" >>~/.bashrc
else
  cd "${pipeline_home}"
  wget "$ccache_download_url"
  file_name="$(basename ${ccache_download_url})"
  tar -xf "$file_name"
  dir_name="${file_name/.tar.xz/}"
  echo "
export CCACHE_DIR=$HOME/.cache/ccache
export PATH=$ROOT/$dir_name:\$PATH" >>~/.bashrc
fi

set +x
# shellcheck source=/dev/null
source ~/.bashrc
set -x
ccache --version

ccache -M 100G
ccache -sv 2>>/dev/null || ccache -s
