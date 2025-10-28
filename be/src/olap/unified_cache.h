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

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "olap/lru_cache.h"
#include "runtime/memory/lru_cache_policy.h"
#include "util/slice.h"

namespace doris {

class UnifiedCacheHandle;

// Unified cache key structure for different cache scenarios
class UnifiedCacheKey {
public:
    // Factory method: Create key for page cache (with offset)
    static UnifiedCacheKey create_page_key(const std::string& fname, size_t fsize,
                                           int64_t offset) {
        UnifiedCacheKey key;
        key._fname = fname;
        key._size = fsize;
        key._offset = offset;
        key._has_offset = true;
        return key;
    }

    // Factory method: Create key for metadata cache (with mtime)
    static UnifiedCacheKey create_metadata_key(const std::string& fname, int64_t mtime,
                                               int64_t fsize) {
        UnifiedCacheKey key;
        key._fname = fname;
        key._size = fsize;
        key._offset = mtime; // Reuse offset field to store mtime
        key._has_offset = true;
        return key;
    }

    // Factory method: Create simple key (string only)
    static UnifiedCacheKey create_simple_key(const std::string& key_str) {
        UnifiedCacheKey key;
        key._fname = key_str;
        key._has_offset = false;
        return key;
    }

    // Encode key to string for cache lookup
    std::string encode() const {
        if (!_has_offset) {
            return _fname;
        }
        std::string encoded;
        encoded.reserve(_fname.size() + sizeof(_size) + sizeof(_offset));
        encoded.append(_fname);
        encoded.append(reinterpret_cast<const char*>(&_size), sizeof(_size));
        encoded.append(reinterpret_cast<const char*>(&_offset), sizeof(_offset));
        return encoded;
    }

private:
    std::string _fname;
    size_t _size = 0;
    int64_t _offset = 0;
    bool _has_offset = false;
};

// Unified cache value wrapper that supports different data types
template <typename T>
class UnifiedCacheValue : public LRUCacheValueBase {
public:
    using ValueType = T;

    explicit UnifiedCacheValue(T data, size_t size = 0) : _data(std::move(data)), _size(size) {}

    ~UnifiedCacheValue() override = default;

    const T& data() const { return _data; }
    T& data() { return _data; }
    size_t size() const { return _size; }

private:
    T _data;
    size_t _size;
};

// Unified cache handle for managing cache entries
class UnifiedCacheHandle {
public:
    UnifiedCacheHandle() = default;
    UnifiedCacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~UnifiedCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    UnifiedCacheHandle(UnifiedCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    UnifiedCacheHandle& operator=(UnifiedCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    bool valid() const { return _cache != nullptr && _handle != nullptr; }

    // Get shared_ptr type data
    template <typename T>
    std::shared_ptr<T> get_shared_ptr() const {
        static_assert(std::is_same<T, typename std::remove_cv<T>::type>::value,
                      "T should not have cv-qualifiers");
        using ValueType = UnifiedCacheValue<std::shared_ptr<T>>;
        auto* value = static_cast<ValueType*>(_cache->value(_handle));
        return value->data();
    }

    // Get raw pointer type data
    template <typename T>
    T* get_ptr() const {
        using ValueType = UnifiedCacheValue<T*>;
        auto* value = static_cast<ValueType*>(_cache->value(_handle));
        return value->data();
    }

    // Get Slice data (for page cache - DataPage is stored directly, not wrapped)
    Slice get_slice() const;

    // Backward compatibility: data() is an alias for get_slice()
    Slice data() const { return get_slice(); }

    // Backward compatibility: get<T>() for shared_ptr types
    template <typename T>
    T get() const {
        static_assert(std::is_same<typename std::remove_cv<T>::type,
                                   std::shared_ptr<typename T::element_type>>::value,
                      "T must be a std::shared_ptr");
        using ValueType = typename T::element_type; // Type that shared_ptr points to
        return get_shared_ptr<ValueType>();
    }

    LRUCachePolicy* cache() const { return _cache; }

private:
    LRUCachePolicy* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    DISALLOW_COPY_AND_ASSIGN(UnifiedCacheHandle);
};

// Unified cache manager - manages all cache instances
class UnifiedCacheManager {
public:
    using CacheType = CachePolicy::CacheType;

    struct CacheConfig {
        CacheType type;
        size_t capacity;
        uint32_t num_shards;
        uint32_t stale_sweep_time_s;
        LRUCacheType lru_type;
        bool enable_prune;

        CacheConfig(CacheType t, size_t cap, uint32_t shards = 16, uint32_t sweep_time = 300,
                    LRUCacheType lt = LRUCacheType::SIZE, bool prune = true)
                : type(t),
                  capacity(cap),
                  num_shards(shards),
                  stale_sweep_time_s(sweep_time),
                  lru_type(lt),
                  enable_prune(prune) {}
    };

    static UnifiedCacheManager* instance() {
        static UnifiedCacheManager mgr;
        return &mgr;
    }

    // Register cache instance
    void register_cache(CacheType type, std::unique_ptr<LRUCachePolicy> cache);

    // Get cache instance
    LRUCachePolicy* get_cache(CacheType type);

    // Lookup operation
    bool lookup(CacheType type, const UnifiedCacheKey& key, UnifiedCacheHandle* handle);

    // Insert shared_ptr
    template <typename T>
    void insert_shared_ptr(CacheType type, const UnifiedCacheKey& key, std::shared_ptr<T> data,
                           size_t size, UnifiedCacheHandle* handle,
                           CachePriority priority = CachePriority::NORMAL) {
        auto* cache = get_cache(type);
        if (!cache) {
            return;
        }

        auto* value = new UnifiedCacheValue<std::shared_ptr<T>>(std::move(data), size);
        auto* h = cache->insert(key.encode(), value, size, size, priority);
        if (h) {
            *handle = UnifiedCacheHandle(cache, h);
        }
    }

    // Insert raw pointer (cache takes ownership and will delete it)
    template <typename T>
    void insert_ptr(CacheType type, const UnifiedCacheKey& key, T* data, size_t size,
                    UnifiedCacheHandle* handle, CachePriority priority = CachePriority::NORMAL) {
        auto* cache = get_cache(type);
        if (!cache) {
            return;
        }

        // Wrap pointer with a deleter that will be called when cache evicts the entry
        auto deleter = [](T* ptr) { delete ptr; };
        std::shared_ptr<T> shared_data(data, deleter);

        auto* value = new UnifiedCacheValue<std::shared_ptr<T>>(std::move(shared_data), size);
        auto* h = cache->insert(key.encode(), value, size, sizeof(T), priority);
        if (h) {
            *handle = UnifiedCacheHandle(cache, h);
        }
    }

private:
    UnifiedCacheManager() = default;

    std::mutex _mutex;
    std::unordered_map<CacheType, std::unique_ptr<LRUCachePolicy>> _caches;
};

} // namespace doris

