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

#include "io/fs/file_metadata_cache.h"

#include <glog/logging.h>

#include "common/config.h"
#include "io/file_factory.h"
#include "runtime/exec_env.h"

namespace doris {

static FileMetadataCache* s_file_metadata_cache_instance = nullptr;

FileMetadataCache* FileMetadataCache::create_global_cache(size_t capacity, uint32_t num_shards) {
    if (s_file_metadata_cache_instance != nullptr) {
        LOG(WARNING) << "FileMetadataCache global instance already exists, returning existing one";
        return s_file_metadata_cache_instance;
    }
    s_file_metadata_cache_instance = new FileMetadataCache(capacity, num_shards);
    return s_file_metadata_cache_instance;
}

FileMetadataCache* FileMetadataCache::instance() {
    return s_file_metadata_cache_instance;
}

FileMetadataCache::FileMetadataCache(size_t capacity, uint32_t num_shards)
        : _capacity(capacity), _num_shards(num_shards) {
    // Use capacity from config if not explicitly provided
    if (capacity == 0) {
        _capacity = std::stoll(config::file_metadata_cache_capacity);
    }
    
    // Create and register the cache with UnifiedCacheManager
    auto cache = std::make_unique<LRUCachePolicy>(
            CacheType::FILE_METADATA_CACHE, _capacity, LRUCacheType::SIZE,
            config::file_metadata_cache_stale_sweep_time_sec, num_shards);
    UnifiedCacheManager::instance()->register_cache(CacheType::FILE_METADATA_CACHE,
                                                    std::move(cache));
    LOG(INFO) << "FileMetadataCache created with capacity: " << _capacity
              << " bytes, num_shards: " << num_shards;
}

std::string FileMetadataCache::get_key(const io::FileReaderSPtr& file_reader,
                                       const io::FileDescription& file_desc) {
    int64_t mtime = file_desc.mtime;
    int64_t size = file_desc.file_size == -1 ? file_reader->size() : file_desc.file_size;
    return UnifiedCacheKey::create_metadata_key(file_reader->path().native(), mtime, size)
            .encode();
}

bool FileMetadataCache::enabled() const {
    auto* cache = UnifiedCacheManager::instance()->get_cache(CacheType::FILE_METADATA_CACHE);
    return cache != nullptr && _capacity > 0;
}

static ParquetMetadataCache* s_parquet_metadata_cache_instance = nullptr;

ParquetMetadataCache* ParquetMetadataCache::instance() {
    if (s_parquet_metadata_cache_instance == nullptr) {
        s_parquet_metadata_cache_instance = new ParquetMetadataCache();
    }
    return s_parquet_metadata_cache_instance;
}

} // namespace doris

