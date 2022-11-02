#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

# TeamCity treats a string surrounded by percentage signs (%) in the script as a parameter reference.
# To prevent TeamCity from treating the text in the percentage signs as a property reference,
# use double percentage signs to escape them.
TIMESTAMP=$(date '+%%Y%%m%%d%%H%%M%%S')
DORIS_SOURCE_CODE_DIR=${DORIS_SOURCE_CODE_DIR:-"/mnt/hdd01/ldy/incubator-doris/"}
DEPLOY_DIR=${DORIS_DEPLOY_DIR:="/mnt/ssd01/Doris-deploy-dir/"}
#fe host
FE_HOST="172.21.0.4"
#3be hosts
BE1_HOST="172.21.0.6"
BE2_HOST="172.21.0.8"
BE3_HOST="172.21.0.15"

echo "--step 1/4, prepare Doirs install package"
cd "$DORIS_SOURCE_CODE_DIR"
branch_name=$(git symbolic-ref --short HEAD)
commit_id=$(git rev-parse --short HEAD)
package_dirname="${branch_name}-${commit_id}-release-${TIMESTAMP}"
tar_file_name="${package_dirname}.tar.gz"
echo "--make Doris tar file ${tar_file_name}"
tar -zcf "${tar_file_name}" \
    --exclude=output/fe/doris-meta/* \
    --exclude=output/be/storage/* \
    --exclude=output/be/lib/meta-tool \
    output/*

echo "--step 2/4, distribute Doris tar file to ${DEPLOY_DIR}"
fe_opts="-H $FE_HOST"
be_opts="-H $BE1_HOST -H $BE2_HOST -H $BE3_HOST"
parallel-scp "$fe_opts" "$be_opts" "${tar_file_name}" "${DEPLOY_DIR}"
parallel-ssh $fe_opts $be_opts -i mkdir -p ${DEPLOY_DIR}/${package_dirname}
parallel-ssh $fe_opts $be_opts -i tar -zxf ${DEPLOY_DIR}/${tar_file_name} -C ${DEPLOY_DIR}/${package_dirname}

echo "--step 3/4, replace file"
bash stop_cluster.sh "$DEPLOY_DIR"
parallel-ssh $fe_opts -i cp -rf ${DEPLOY_DIR}/${package_dirname}/output/fe/lib/* ${DEPLOY_DIR}/fe/lib/
parallel-ssh $fe_opts -i cp -rf ${DEPLOY_DIR}/${package_dirname}/output/fe/bin/* ${DEPLOY_DIR}/fe/bin/
parallel-ssh $be_opts -i cp -rf ${DEPLOY_DIR}/${package_dirname}/output/be/lib/* ${DEPLOY_DIR}/be/lib/
parallel-ssh $be_opts -i cp -rf ${DEPLOY_DIR}/${package_dirname}/output/be/bin/* ${DEPLOY_DIR}/be/bin/

echo "--step 4/4, start new Doris"
bash start_cluster.sh "$DEPLOY_DIR"
bash wait_cluster_ready.sh

echo "==========deploy done.=========="
