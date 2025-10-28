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

#include "io/fs/file_reader_writer_fwd.h"
#include "olap/unified_cache.h"

namespace doris {

namespace io {
struct FileDescription;
} // namespace io

// File metadata cache - a generic cache for file metadata (Parquet footer, ORC footer, etc.)
class FileMetadataCache {
public:
    using CacheType = CachePolicy::CacheType;

    // Create global instance
    static FileMetadataCache* create_global_cache(size_t capacity, uint32_t num_shards = 16);

    // Get global instance
    static FileMetadataCache* instance();

    FileMetadataCache(size_t capacity, uint32_t num_shards = 16);

    // Generic lookup method
    bool lookup(const std::string& path, int64_t mtime_or_version, int64_t size,
                UnifiedCacheHandle* handle) {
        auto key = UnifiedCacheKey::create_metadata_key(path, mtime_or_version, size);
        return UnifiedCacheManager::instance()->lookup(CacheType::FILE_METADATA_CACHE, key,
                                                       handle);
    }

    // Generic insert method (shared_ptr version)
    template <typename T>
    void insert(const std::string& path, int64_t mtime_or_version, int64_t size,
                std::shared_ptr<T> metadata, size_t metadata_size, UnifiedCacheHandle* handle) {
        auto key = UnifiedCacheKey::create_metadata_key(path, mtime_or_version, size);
        UnifiedCacheManager::instance()->insert_shared_ptr(CacheType::FILE_METADATA_CACHE, key,
                                                           std::move(metadata), metadata_size,
                                                           handle);
    }

    // Convenience method: Generate key from FileReader
    static std::string get_key(const io::FileReaderSPtr& file_reader,
                               const io::FileDescription& file_desc);

    bool enabled() const;

private:
    size_t _capacity;
    uint32_t _num_shards;
};

// Parquet-specific convenience wrapper
class ParquetMetadataCache {
public:
    static ParquetMetadataCache* instance();

    ParquetMetadataCache() = default;

    // Parquet-specific lookup interface
    template <typename MetadataType>
    bool lookup(const io::FileReaderSPtr& file_reader, const io::FileDescription& file_desc,
                UnifiedCacheHandle* handle) {
        int64_t mtime = file_desc.mtime;
        int64_t size = file_desc.file_size == -1 ? file_reader->size() : file_desc.file_size;
        return FileMetadataCache::instance()->lookup(file_reader->path().native(), mtime, size,
                                                     handle);
    }

    // Parquet-specific insert interface
    template <typename MetadataType>
    void insert(const io::FileReaderSPtr& file_reader, const io::FileDescription& file_desc,
                std::shared_ptr<MetadataType> metadata, size_t metadata_size,
                UnifiedCacheHandle* handle) {
        int64_t mtime = file_desc.mtime;
        int64_t size = file_desc.file_size == -1 ? file_reader->size() : file_desc.file_size;
        FileMetadataCache::instance()->insert(file_reader->path().native(), mtime, size,
                                              std::move(metadata), metadata_size, handle);
    }
};

} // namespace doris

