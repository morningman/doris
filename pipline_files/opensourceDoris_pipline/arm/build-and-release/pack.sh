#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

target_branch=%target_branch%
tartget_commit_id=%tartget_commit_id%
output_package_name=%output_package_name%
teamcity_build_checkoutDir=%teamcity.build.checkoutDir%

# TeamCity treats a string surrounded by percentage signs (%) in the script as a parameter reference.
# To prevent TeamCity from treating the text in the percentage signs as a property reference,
# use double percentage signs to escape them.
TIMESTAMP=$(date '+%%Y%%m%%d%%H%%M%%S')

if [[ -z "$target_branch" ]] ||
    [[ -z "$tartget_commit_id" ]] ||
    [[ -z "$output_package_name" ]]; then
    echo "
Please set all parameters:
target_branch: $target_branch
tartget_commit_id: $tartget_commit_id
output_package_name: $output_package_name
" && exit 1
fi

actual_branch=$(git symbolic-ref --short HEAD)
actual_commit_id=$(git log -1 --pretty='%h')
if [[ "$actual_branch" != "$target_branch" ]] ||
    [[ "$actual_commit_id" != "$tartget_commit_id"* ]]; then
    echo 'checkout FAIL...' && exit 1
else
    echo "checkout SUCCESS..."
fi

if [[ ! -d 'output' ]]; then echo 'output dir missed...' && exit 1; fi

cd 'output'
if ! tar -zcf "$TIMESTAMP"_"$output_package_name".tar.gz .*; then echo 'make tar file fail...' && exit 1; fi
bash install-coscmd.sh
coscmd upload \
    "$TIMESTAMP"_"$output_package_name".tar.gz \
    teamcity-release/"$TIMESTAMP"_"$output_package_name".tar.gz
url=$(coscmd signurl teamcity-release/"$TIMESTAMP"_"$output_package_name".tar.gz | cut -d'?' -f 1 | cut -d"'" -f2)

echo "$url"
