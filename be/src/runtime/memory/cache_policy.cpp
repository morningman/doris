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

#include "runtime/memory/cache_policy.h"

#include "runtime/memory/cache_manager.h"

namespace doris {
#include "common/compile_check_begin.h"

CachePolicy::CachePolicy(CacheType type, size_t capacity, uint32_t stale_sweep_time_s,
                         bool enable_prune)
        : _type(type),
          _initial_capacity(capacity),
          _stale_sweep_time_s(stale_sweep_time_s),
          _enable_prune(enable_prune) {
    init_profile();
}

CachePolicy::~CachePolicy() {
    CacheManager::instance()->unregister_cache(_type);
}

#include "common/compile_check_end.h"
} // namespace doris
