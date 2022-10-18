#!/bin/bash

# shellcheck source=/dev/null
source ~/.bashrc

set -ex

if coscli --version; then
  coscli --version
  exit
fi

# os type: linux, mac
os_type='linux'
if [[ -n "$1" ]]; then os_type="$1"; fi

if [[ "$os_type" == 'linux' ]]; then
  wget https://github.com/tencentyun/coscli/releases/download/v0.12.0-beta/coscli-linux
  mv coscli-linux coscli
elif [[ "$os_type" == 'mac' ]]; then
  wget https://github.com/tencentyun/coscli/releases/download/v0.12.0-beta/coscli-mac
  mv coscli-mac coscli
else
  echo 'Only linux or mac.'
  exit 1
fi
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

PATH="$(pwd):$PATH"
export PATH
echo "export PATH=$(pwd):\$PATH" >>~/.bashrc
coscli --version

# max default shell is zsh
if [[ "$os_type" == 'mac' ]]; then
  # mac load order: /etc/profile /etc/paths ~/.bash_profile ~/.bash_login ~/.profile ~/.bashrc
  echo "
if [ -f ~/.bashrc ]; then
   source ~/.bashrc
fi" >>~/.zshrc
fi
