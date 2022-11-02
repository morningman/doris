#!/bin/bash
# shellcheck source=/dev/null
source ~/.bashrc

set -ex

# TeamCity treats a string surrounded by percentage signs (%) in the script as a parameter reference.
# To prevent TeamCity from treating the text in the percentage signs as a property reference,
# use double percentage signs to escape them.
TIMESTAMP=$(date '+%%Y%%m%%d%%H%%M%%S')
DORIS_SOURCE_CODE_DIR=${DORIS_SOURCE_CODE_DIR:-"/mnt/hdd01/ldy/incubator-doris/"}

echo "#### compile doris with docker ####"
cd "$DORIS_SOURCE_CODE_DIR"
docker pull apache/doris:build-env-ldb-toolchain-latest
if docker run -t --rm \
    --name doris_compile_"${TIMESTAMP}" \
    -e BUILD_TYPE=release \
    -e BUILD_META_TOOL=off \
    -e BUILD_JAVA_UDF=off \
    -e USE_JEMALLOC=ON \
    -v "$HOME"/.m2:/root/.m2 \
    -v "$HOME"/.ccache:/root/.ccache \
    -v "${DORIS_SOURCE_CODE_DIR}":"${DORIS_SOURCE_CODE_DIR}" \
    docker.io/apache/doris:build-env-ldb-toolchain-latest \
    bash -c "${DORIS_SOURCE_CODE_DIR}/build.sh --fe --be -j$(nproc)"; then
    if [ -d 'output' ]; then
        echo -e "
\033[32m
##########################
compile doris, PASS
##########################
\033[0m" && exit 0
    fi
fi

echo -e "
\033[31m
##########################
compile doris, FAIL
##########################
\033[0m" && exit 1
