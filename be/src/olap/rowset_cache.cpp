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

#include "olap/rowset_cache.h"

#include "olap/rowset/rowset.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"

namespace doris {

RowsetCache* RowsetCache::_s_instance = nullptr;

void RowsetCache::create_global_cache(size_t capacity) {
    DCHECK(_s_instance == nullptr);
    static RowsetCache instance(capacity);
    _s_instance = &instance;
}

RowsetCache::RowsetCache(size_t capacity)
    : _mem_tracker(MemTracker::CreateTracker(capacity, "RowsetCache", nullptr, true, true, MemTrackerLevel::OVERVIEW)) {
        _cache = std::unique_ptr<Cache>(new_lru_cache("RowsetCache", capacity, _mem_tracker));
}

bool RowsetCache::lookup(const CacheKey& key, RowsetCacheHandle* handle) {
    auto lru_handle = _cache->lookup(key.encode());
    if (lru_handle == nullptr) {
        return false;
    }
    *handle = RowsetCacheHandle(_cache.get(), lru_handle);
    // LOG(INFO) << "cmy look up ok";
    return true;
}

void RowsetCache::insert(const CacheKey& key, RowsetCacheValue& value, RowsetCacheHandle* handle) {
    auto deleter = [](const doris::CacheKey& key, void* value) {
        RowsetCacheValue* cache_value = (RowsetCacheValue* ) value;
        TabletManager* manager = ExecEnv::GetInstance()->storage_engine()->tablet_manager();
        RowsetSharedPtr rowset;
        TabletSharedPtr tablet = manager->get_tablet(cache_value->tablet_id, cache_value->schema_hash);
        do {
            if (tablet == nullptr) {
                LOG(WARNING) << "can not find tablet when deleting rowset cache: " << cache_value->tablet_id;
                break;
            }

            ReadLock rdlock(tablet->get_header_lock_ptr());
            rowset = tablet->get_rowset_by_version(cache_value->version, true);
            if (rowset == nullptr) {
                LOG(WARNING) << "can not find rowset with version " << cache_value->version
                        << " in tablet when deleting rowset cache: " << cache_value->tablet_id;
                break;
            }
        } while (false);

        if (rowset != nullptr) {
            OLAPStatus st = rowset->unload();
            if (st != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to unload rowset with version " << cache_value->version
                        << " in tablet when deleting rowset cache: " << cache_value->tablet_id;
            }
        }

        delete cache_value;
    };

    LOG(INFO) << "cmy insert rowset cache mem footprint: " << value.mem_footprint;
    auto lru_handle = _cache->insert(key.encode(), &value, sizeof(RowsetCacheValue) + value.mem_footprint, deleter, CachePriority::NORMAL);
    *handle = RowsetCacheHandle(_cache.get(), lru_handle);
}

OLAPStatus RowsetCache::load_rowset(const RowsetSharedPtr& rowset, RowsetCacheHandle* cache_handle,
            std::shared_ptr<MemTracker> parent_tracker) {
    RowsetCacheHandle handle;
    RowsetCache::CacheKey cache_key(rowset->rowset_id());
    if (lookup(cache_key, &handle)) {
        *cache_handle = std::move(handle);
        return OLAP_SUCCESS;
    }

    RETURN_NOT_OK(rowset->load(true, parent_tracker));
    
    // memory of RowsetCacheValue will be handled by rowset cache
    RowsetCacheValue* cache_value = new RowsetCacheValue(
            rowset->rowset_meta()->tablet_id(),
            rowset->rowset_meta()->tablet_schema_hash(),
            rowset->version(),
            rowset->mem_footprint()); 
    insert(cache_key, *cache_value, &handle);
    *cache_handle = std::move(handle);
    return OLAP_SUCCESS;
}

} // namespace doris
