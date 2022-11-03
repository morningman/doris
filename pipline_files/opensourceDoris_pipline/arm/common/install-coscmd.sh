#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

if which coscmd; then exit 0; fi

if ! wget 'https://bootstrap.pypa.io/pip/3.6/get-pip.py'; then echo 'download pip fail...' && exit 1; fi
python get-pip.py || exit 1
pip install coscmd || exit 1
# coscmd --version return code 255??? maybe it has bug on aarch64, but not effect upload file.
set +e
coscmd --version
set -e

echo "[common]
secret_id = AKIDznMfiUzDBrGOGrBF2YwAB2g8NN8drqY0
secret_key = DArHQIuGLAizzhGYBYeU3LL6aniMkrhG
bucket = doris-build-1308700295
region = ap-beijing
#bucket = doris-community-test-1308700295
#region = ap-hongkong
max_thread = 5
part_size = 1
retry = 5
timeout = 60
schema = https
verify = md5
anonymous = False" >~/.cos.conf

if coscmd list; then
  echo 'install coscmd SUCCESS...'
else
  echo 'install coscmd FAIL...' && exit 1
fi
