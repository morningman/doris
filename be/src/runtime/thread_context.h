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

#include <iostream>
#include <string>
#include <thread>

#include "util/thread_task.h"
#include "gen_cpp/Types_types.h"

namespace doris {

// The thread context saves some info about a working thread.
// 2 requried info:
//   1. thread_id:   Current thread id, Auto generated.
//   2. type:        The type is a enum value indicating which type of task current thread is running.
//                   For example: QUERY, LOAD, COMPACTION, ...
//   3. task id:     A unique id to identify this task. maybe query id, load job id, etc.
//
// There may be other optional info to be added later.
class ThreadContext {
public:
    ThreadContext() : _thread_id(std::this_thread::get_id()), _type(ThreadTask::Type::UNKNOWN) {}
    ~ThreadContext() {}

    void attach(ThreadTask::Type type, const std::string& task_id,
                const TUniqueId& fragment_instance_id = TUniqueId()) {
        _type = type;
        _task_id = task_id;
        _fragment_instance_id = fragment_instance_id;
    }

    void detach() {
        _type = ThreadTask::Type::UNKNOWN;
        _task_id = "";
        _fragment_instance_id = TUniqueId();
    }

    std::string type();
    const std::string& task_id() const { return _task_id; }
    const std::thread::id& thread_id() { return _thread_id; }

    friend std::ostream& operator<<(std::ostream& os, const ThreadContext& ctx);

private:
    std::thread::id _thread_id;
    ThreadTask::Type _type;
    std::string _task_id;
    TUniqueId _fragment_instance_id;
};

inline thread_local ThreadContext thread_local_ctx;

inline std::string ThreadContext::type() {
    return TASK_TYPE_STRING(_type);
}

} // namespace doris
