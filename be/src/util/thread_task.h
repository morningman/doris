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

#pragma once

#include <functional>
#include <string>

namespace doris {

typedef std::function<void()> WorkFunction;

struct ThreadTask {
public:
    enum Type {
        UNKNOWN = 0,
        QUERY,
        LOAD,
        COMPACTION,
        AGENT,
        HTTP_WORKER,
        MIGRATION,
        CHECKPOINT
        // to be added
    };

public:
    std::string task_id;
    Type type;
    // the priority is only used if this task is submitted to the priority thread pool
    int priority = 0;

public:
    WorkFunction work_function;
    bool operator<(const ThreadTask& o) const { return priority < o.priority; }

    ThreadTask& operator++() {
        priority += 2;
        return *this;
    }
};

inline const std::string TASK_TYPE_STRING(ThreadTask::Type type) {
    switch (type) {
    case ThreadTask::Type::QUERY:
        return "QUERY";
    case ThreadTask::Type::LOAD:
        return "LOAD";
    case ThreadTask::Type::COMPACTION:
        return "COMPACTION";
    default:
        return "UNKNOWN";
    }
}

}; // namespace doris
