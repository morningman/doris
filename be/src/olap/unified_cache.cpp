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

#include "olap/unified_cache.h"

#include <glog/logging.h>

namespace doris {

void UnifiedCacheManager::register_cache(CacheType type, std::unique_ptr<LRUCachePolicy> cache) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_caches.find(type) != _caches.end()) {
        LOG(WARNING) << "Cache type " << CachePolicy::type_string(type)
                     << " is already registered, overwriting";
    }
    _caches[type] = std::move(cache);
}

LRUCachePolicy* UnifiedCacheManager::get_cache(CacheType type) {
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _caches.find(type);
    if (it != _caches.end()) {
        return it->second.get();
    }
    return nullptr;
}

bool UnifiedCacheManager::lookup(CacheType type, const UnifiedCacheKey& key,
                                 UnifiedCacheHandle* handle) {
    auto* cache = get_cache(type);
    if (!cache) {
        return false;
    }

    auto* h = cache->lookup(key.encode());
    if (h != nullptr) {
        *handle = UnifiedCacheHandle(cache, h);
        return true;
    }
    return false;
}

} // namespace doris

