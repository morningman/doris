#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc
set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%
# teamcity_build_checkoutDir='/root/teamcity/teamcity-agent/work/e0bc234628561cad'

DORIS_HOME="$teamcity_build_checkoutDir/output/"

skip_pipeline=${skip_pipeline:="false"}
tmp_env_file_path="$teamcity_build_checkoutDir/.my_tmp_env"
if [[ -f $"$tmp_env_file_path" ]]; then source "$tmp_env_file_path"; fi
echo '####check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then echo "skip build pipline" && exit 0; fi

echo "####stop doris"
set +e
bash switch-cluster.sh "$DORIS_HOME" off
set -e

echo "####clean DONE."
