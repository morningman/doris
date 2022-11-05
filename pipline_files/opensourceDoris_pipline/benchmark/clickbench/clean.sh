#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc
set -e

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%

skip_pipeline=${skip_pipeline:="false"}
tmp_env_file_path="$teamcity_build_checkoutDir/.my_tmp_env"
if [[ -f $"$tmp_env_file_path" ]]; then source "$tmp_env_file_path"; fi
echo '####check if skip'
if [[ "${skip_pipeline}" == "true" ]]; then echo "skip build pipline" && exit 0; fi

echo "####stop doris"
set +e
bash kill-doris-cluster.sh
set -e

echo "####clean DONE."
