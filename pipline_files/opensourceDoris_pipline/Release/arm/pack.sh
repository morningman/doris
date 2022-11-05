#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

output_package_name=%output_package_name%

# TeamCity treats a string surrounded by percentage signs (%) in the script as a parameter reference.
# To prevent TeamCity from treating the text in the percentage signs as a property reference,
# use double percentage signs to escape them.
TIMESTAMP=$(date '+%%Y%%m%%d%%H%%M%%S')

# output_package_name='doris-1.1.1-bin-arm'
# TIMESTAMP='202210241024'

bash install-coscmd.sh

if [[ -z "$output_package_name" ]]; then echo "Invalid output_package_name" && exit 1; fi
actual_output_package_name="${TIMESTAMP}_${output_package_name}.tar.gz"
if [[ ! -d 'output' ]]; then echo 'output dir missed...' && exit 1; fi

cd 'output'

if ! tar -zcf \
    "$actual_output_package_name" \
    --exclude=fe/doris-meta/* \
    --exclude=fe/log/* \
    --exclude=be/storage/* \
    --exclude=be/log/* \
    ./*; then
    echo 'make tar file fail...' && exit 1
fi
coscmd upload \
    "$actual_output_package_name" \
    teamcity-release/"$actual_output_package_name"
url=$(coscmd signurl teamcity-release/"$actual_output_package_name" | cut -d'?' -f1 | cut -d"'" -f2)

echo -e "
=================================================

$url

=================================================
"
