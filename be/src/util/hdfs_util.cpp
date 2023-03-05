// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/hdfs_util.h"

#include <util/string_util.h>

#include <hdfs2/hdfs.h>

#include "common/config.h"
#include "common/logging.h"

namespace doris {

std::string get_hdfs_error() {
    int e = errno;
    if (e == 0) {
        return "";
    }
    std::stringstream ss;
    char buf[1024];
    ss << "Error(" << e << "): " << strerror_r(e, buf, 1024);
    char* root_cause = hdfsGetLastExceptionRootCause();
    if (root_cause != nullptr) {
        ss << ", root_cause=" << root_cause;
    }
    return ss.str();
}

} // namespace doris
