#!/bin/bash
set -ex

# shellcheck source=/dev/null
source ~/.bashrc

if coscli --version; then
  coscli --version
  exit
fi

wget https://github.com/tencentyun/coscli/releases/download/v0.12.0-beta/coscli-linux
mv coscli-linux coscli
chmod 755 coscli
./coscli --version

echo -e "
cos:
  base:
    secretid: ElCqnG/RRj/6L+zs/QQIuGJdHJiDWsAHGv8rt10I234w5nXEnTlL0e5WhIozxziZ
    secretkey: oxb+GiuzhIhwpp/eZcpUhfyAnvwiSUI5fMAKwrBcDKXeQ1lHjB+NSA6psd9+JBIz
    sessiontoken: 3kNZR4wfjUgOqbHffiQSMw==
    protocol: https
  buckets:
  - name: doris-community-test-1308700295
    alias: hk
    region: \"\"
    endpoint: cos.ap-hongkong.myqcloud.com
" >~/.cos.yaml

#TODO, not effect?
echo "
export PATH=$(pwd):\$PATH
" >>~/.bashrc

# shellcheck source=/dev/null
source ~/.bashrc
