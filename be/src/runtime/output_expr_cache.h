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
#include <utility>
#include <string>
#include <unordered_map>
#include <mutex>

#include "common/status.h"
#include "runtime/runtime_state.h"

namespace doris {

class OutputExprCache {
public:
    
    OutputExprCache() {
        TQueryGlobals qg;
        _state = new RuntimeState(qg);
    }

    ~OutputExprCache() {
        _cache.clear();
        delete _state;
    }

    const std::vector<ExprContext*>* get_cached_expr_ctxs(const std::string& key) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto search = _cache.find(key);
        if (search != _cache.end()) {
            return &search->second;
        }
        return nullptr;
    }

    void insert_expr_ctxs_cache(const std::string& key, const std::vector<ExprContext*>& ctxs) {
        std::lock_guard<std::mutex> lock(_mutex);
        std::vector<ExprContext*> new_contexts;
        Expr::clone_if_not_exists(ctxs, _state, &new_contexts);
        _cache.emplace(std::make_pair(key, new_contexts));
    }

private:
    std::mutex _mutex;
    std::unordered_map<std::string, std::vector<ExprContext*>> _cache;
    RuntimeState* _state;
};

} // namespace doris
