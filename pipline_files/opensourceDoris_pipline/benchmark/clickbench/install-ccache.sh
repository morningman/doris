#!/bin/bash
set -ex

# shellcheck source=/dev/null
source ~/.bashrc

if ccache --version; then
  ccache -s
  exit
fi

sudo yum install -y ccache
ccache --version

# echo "
# export CCACHE_DIR=$(pwd)/.ccache
# " >>~/.bashrc

# shellcheck source=/dev/null
source ~/.bashrc

ccache -M 10G
ccache -s
