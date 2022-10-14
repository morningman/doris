#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

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

ccache -M 100G
ccache -s
