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

#include <butil/macros.h>
#include <gen_cpp/segment_v2.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <utility>

#include "olap/lru_cache.h"
#include "olap/unified_cache.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/slice.h"
#include "vec/common/allocator.h"
#include "vec/common/allocator_fwd.h"

namespace doris {

// Forward declaration - use UnifiedCacheHandle as PageCacheHandle
using PageCacheHandle = UnifiedCacheHandle;

template <typename T>
class MemoryTrackedPageBase : public LRUCacheValueBase {
public:
    MemoryTrackedPageBase() = default;
    MemoryTrackedPageBase(size_t b, bool use_cache, segment_v2::PageTypePB page_type);

    MemoryTrackedPageBase(const MemoryTrackedPageBase&) = delete;
    MemoryTrackedPageBase& operator=(const MemoryTrackedPageBase&) = delete;
    ~MemoryTrackedPageBase() = default;

    T data() { return _data; }
    size_t size() { return _size; }

protected:
    T _data;
    size_t _size = 0;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker_by_allocator;
};

class MemoryTrackedPageWithPageEntity : Allocator<false>, public MemoryTrackedPageBase<char*> {
public:
    MemoryTrackedPageWithPageEntity(size_t b, bool use_cache, segment_v2::PageTypePB page_type);

    size_t capacity() { return this->_capacity; }

    ~MemoryTrackedPageWithPageEntity() override;

    void reset_size(size_t n) {
        DCHECK(n <= this->_capacity);
        this->_size = n;
    }

private:
    size_t _capacity = 0;
};

template <typename T>
class MemoryTrackedPageWithPagePtr : public MemoryTrackedPageBase<std::shared_ptr<T>> {
public:
    MemoryTrackedPageWithPagePtr(size_t b, segment_v2::PageTypePB page_type);

    ~MemoryTrackedPageWithPagePtr() override;

    void set_data(std::shared_ptr<T> data) { this->_data = data; }
};

using SemgnetFooterPBPage = MemoryTrackedPageWithPagePtr<segment_v2::SegmentFooterPB>;
using DataPage = MemoryTrackedPageWithPageEntity;

// Wrapper around Cache, and used for cache page of column data
// in Segment. Now refactored to use UnifiedCacheManager.
class StoragePageCache {
public:
    using CacheType = CachePolicy::CacheType;

    // The unique key identifying entries in the page cache.
    // Each cached page corresponds to a specific offset within
    // a file.
    struct CacheKey {
        CacheKey(std::string fname_, size_t fsize_, int64_t offset_)
                : fname(std::move(fname_)), fsize(fsize_), offset(offset_) {}
        std::string fname;
        size_t fsize;
        int64_t offset;

        // Encode to a flat binary which can be used as LRUCache's key
        std::string encode() const {
            return to_unified_key().encode();
        }

        // Convert to UnifiedCacheKey
        UnifiedCacheKey to_unified_key() const {
            return UnifiedCacheKey::create_page_key(fname, fsize, offset);
        }
    };

    class DataPageCache : public LRUCachePolicy {
    public:
        DataPageCache(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::DATA_PAGE_CACHE, capacity,
                                 LRUCacheType::SIZE, config::data_page_cache_stale_sweep_time_sec,
                                 num_shards, DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY, true, true) {
        }
    };

    class IndexPageCache : public LRUCachePolicy {
    public:
        IndexPageCache(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::INDEXPAGE_CACHE, capacity,
                                 LRUCacheType::SIZE, config::index_page_cache_stale_sweep_time_sec,
                                 num_shards) {}
    };

    class PKIndexPageCache : public LRUCachePolicy {
    public:
        PKIndexPageCache(size_t capacity, uint32_t num_shards)
                : LRUCachePolicy(CachePolicy::CacheType::PK_INDEX_PAGE_CACHE, capacity,
                                 LRUCacheType::SIZE,
                                 config::pk_index_page_cache_stale_sweep_time_sec, num_shards) {}
    };

    static constexpr uint32_t kDefaultNumShards = 16;

    // Create global instance of this class
    static StoragePageCache* create_global_cache(size_t capacity, int32_t index_cache_percentage,
                                                 int64_t pk_index_cache_capacity,
                                                 uint32_t num_shards = kDefaultNumShards);

    // Return global instance.
    // Client should call create_global_cache before.
    static StoragePageCache* instance() { return ExecEnv::GetInstance()->get_storage_page_cache(); }

    StoragePageCache(size_t capacity, int32_t index_cache_percentage,
                     int64_t pk_index_cache_capacity, uint32_t num_shards);

    // Lookup the given page in the cache.
    //
    // If the page is found, the cache entry will be written into handle.
    // PageCacheHandle will release cache entry to cache when it
    // destructs.
    //
    // Cache type selection is determined by page_type argument
    //
    // Return true if entry is found, otherwise return false.
    bool lookup(const CacheKey& key, PageCacheHandle* handle, segment_v2::PageTypePB page_type) {
        CacheType cache_type = _page_type_to_cache_type(page_type);
        return UnifiedCacheManager::instance()->lookup(cache_type, key.to_unified_key(), handle);
    }

    // Insert a page with key into this cache.
    // Given handle will be set to valid reference.
    // This function is thread-safe, and when two clients insert two same key
    // concurrently, this function can assure that only one page is cached.
    // The in_memory page will have higher priority.
    void insert(const CacheKey& key, DataPage* data, PageCacheHandle* handle,
                segment_v2::PageTypePB page_type, bool in_memory = false);

    // Insert a std::share_ptr which points to a page into this cache.
    // size should be the size of the page instead of shared_ptr.
    // Internal implementation will wrap shared_ptr with MemoryTrackedPageWithPagePtr
    // Since we are using std::shared_ptr, so lify cycle of the page is not managed by
    // this cache alone.
    // User could store a weak_ptr to the page, and lock it when needed.
    // See Segment::_get_segment_footer for example.
    template <typename T>
    void insert(const CacheKey& key, T data, size_t size, PageCacheHandle* handle,
                segment_v2::PageTypePB page_type, bool in_memory = false);

    std::shared_ptr<MemTrackerLimiter> mem_tracker(segment_v2::PageTypePB page_type) {
        CacheType cache_type = _page_type_to_cache_type(page_type);
        auto* cache = UnifiedCacheManager::instance()->get_cache(cache_type);
        return cache ? cache->mem_tracker() : nullptr;
    }

private:
    StoragePageCache();

    int32_t _index_cache_percentage = 0;
    std::unique_ptr<DataPageCache> _data_page_cache;
    std::unique_ptr<IndexPageCache> _index_page_cache;
    // Cache data for primary key index data page, seperated from data
    // page cache to make it for flexible. we need this cache When construct
    // delete bitmap in unique key with mow
    std::unique_ptr<PKIndexPageCache> _pk_index_page_cache;

    // Helper function to convert page type to cache type
    static CacheType _page_type_to_cache_type(segment_v2::PageTypePB page_type) {
        switch (page_type) {
        case segment_v2::DATA_PAGE:
            return CacheType::DATA_PAGE_CACHE;
        case segment_v2::INDEX_PAGE:
            return CacheType::INDEXPAGE_CACHE;
        case segment_v2::PRIMARY_KEY_INDEX_PAGE:
            return CacheType::PK_INDEX_PAGE_CACHE;
        default:
            throw Exception(Status::InternalError("Unknown page type"));
        }
    }

    LRUCachePolicy* _get_page_cache(segment_v2::PageTypePB page_type) {
        switch (page_type) {
        case segment_v2::DATA_PAGE: {
            return _data_page_cache.get();
        }
        case segment_v2::INDEX_PAGE: {
            return _index_page_cache.get();
        }
        case segment_v2::PRIMARY_KEY_INDEX_PAGE: {
            return _pk_index_page_cache.get();
        }
        default:
            throw Exception(Status::FatalError("get error type page cache"));
        }
        throw Exception(Status::FatalError("__builtin_unreachable"));
    }
};

// Helper function to get Slice data from PageCacheHandle (for backward compatibility)
// This is used in page_cache.cpp
inline Slice page_cache_handle_data(const PageCacheHandle& handle) {
    return handle.get_slice();
}

// Helper function to get shared_ptr data from PageCacheHandle (for backward compatibility)
template <typename T>
inline std::shared_ptr<T> page_cache_handle_get(const PageCacheHandle& handle) {
    return handle.get_shared_ptr<T>();
}

} // namespace doris
