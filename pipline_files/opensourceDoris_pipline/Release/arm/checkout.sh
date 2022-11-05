#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

target_branch=%target_branch%
tartget_commit_id=%tartget_commit_id%
output_package_name=%output_package_name%

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

git fetch
git checkout "$target_branch"
git pull
git reset --hard "$tartget_commit_id"
actual_branch=$(git symbolic-ref --short HEAD)
actual_commit_id=$(git log -1 --pretty='%H')
if [[ "$actual_branch" != "$target_branch" ]] ||
    [[ "$actual_commit_id" != "$tartget_commit_id" ]]; then
    echo 'checkout FAIL...' && exit 1
else
    echo "checkout SUCCESS..."
fi
