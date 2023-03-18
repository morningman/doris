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

#include <system_error>
#include <string.h>
#include <io/fs/hdfs.h>
#include <util/string_util.h>

namespace doris {
namespace io {

std::string get_hdfs_error() {
    std::stringstream ss;
    char buf[1024];
    ss << "(" << errno << "), " << strerror_r(errno, buf, 1024) << ")";
#ifdef USE_HADOOP_HDFS
    char* root_cause = hdfsGetLastExceptionRootCause();
    if (root_cause != nullptr) {
        ss << ", reason: " << root_cause;
    }
#else
    ss << ", reason: " << hdfsGetLastError();
#endif
    return ss.str();
}

Path convert_path(const Path& path, const std::string& namenode) {
    Path real_path(path);
    if (path.string().find(namenode) != std::string::npos) {
        std::string real_path_str = path.string().substr(namenode.size());
        real_path = real_path_str;
    }
    return real_path;
}

} // namespace io
} // namespace doris
