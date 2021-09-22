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

#include <memory>
#include <string>
#include <utility>

#include "gutil/macros.h" // for DISALLOW_COPY_AND_ASSIGN
#include "olap/lru_cache.h"
#include "olap/olap_common.h" // for rowset id
#include "olap/rowset/rowset.h"
#include "runtime/mem_tracker.h"

namespace doris {

class RowsetCacheHandle;
class RowsetCacheValue;

// Wrapper around Cache, and used for rowset cache.
// Because the loaded rowset consumes memory resources, we use lru cache to cache a limited number of rowset.
// For objects that need to read rowset data (such as rowset_reader), they should be accessed in the following mode:
//
// ...
// RowsetCacheHandle rowset_handle;
// rowset->load(&rowset_handle, ...);
// _rowset_handle = std::move(rowset_handle);
// ...
//
// The _rowset_handle should be the same as the life cycle of rowset_reader
// to ensure that the rowset will not be unloaded during the entire access period.
class RowsetCache {
public:
    struct CacheKey {
        CacheKey(RowsetId rowset_id_) : rowset_id(rowset_id_) {}
        RowsetId rowset_id;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            return rowset_id.to_string();
        }
    };

    // Create global instance of this class
    static void create_global_cache(size_t capacity);

    // Return global instance.
    // Client should call create_global_cache before.
    static RowsetCache* instance() { return _s_instance; }

    RowsetCache(size_t capacity);

    // Lookup the given rowset in the cache.
    // If the rowset is found, the cache entry will be written into handle.
    // Return true if entry is found, otherwise return false.
    bool lookup(const CacheKey& key, RowsetCacheHandle* handle);

    // Insert a cache entry by key.
    // And the cache entry will be returned in handle.
    // This function is thread-safe.
    void insert(const CacheKey& key, RowsetCacheValue& value, RowsetCacheHandle* handle);

    OLAPStatus load_rowset(const RowsetSharedPtr& rowset, RowsetCacheHandle* cache_handle,
            std::shared_ptr<MemTracker> parent_tracker = nullptr);

private:
    RowsetCache();
    static RowsetCache* _s_instance;

    std::unique_ptr<Cache> _cache = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
};

// A handle for a single rowset from rowset lru cache.
// The handle can ensure that the rowset remains in the LOADED state
// and will not be closed while the holder of the handle is accessing the rowset.
// The handle will automatically release the cache entry when it is destroyed.
// So the caller need to make sure the handle is valid in lifecycle.
class RowsetCacheHandle {
public:
    RowsetCacheHandle() {}
    RowsetCacheHandle(Cache* cache, Cache::Handle* handle) : _cache(cache), _handle(handle) {}
    ~RowsetCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    RowsetCacheHandle(RowsetCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    RowsetCacheHandle& operator=(RowsetCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    Cache* cache() const { return _cache; }

private:
    Cache* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(RowsetCacheHandle);
};

struct RowsetCacheValue {
    RowsetCacheValue(int64_t tablet_id_, int32_t schema_hash_, const Version& version_):
            tablet_id(tablet_id_),
            schema_hash(schema_hash_),
            version(version_) {}
    int64_t tablet_id;
    int32_t schema_hash; 
    Version version;
};

} // namespace doris
