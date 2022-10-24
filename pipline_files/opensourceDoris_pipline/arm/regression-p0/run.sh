#!/bin/bash

set -ex

teamcity_build_checkoutDir=%teamcity.build.checkoutDir%

DORIS_HOME="$teamcity_build_checkoutDir/output/"

# edit regression conf

bash run-regression-test.sh --teamcity --clean --run -parallel 3 -suiteParallel 3 -actionParallel 3 -g p0
