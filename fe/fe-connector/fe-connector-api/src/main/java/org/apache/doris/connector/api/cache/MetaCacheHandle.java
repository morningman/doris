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

package org.apache.doris.connector.api.cache;

import java.util.Objects;
import java.util.Optional;

/**
 * Handle to a cache instance backing a {@link ConnectorMetaCacheBinding}.
 *
 * <p>Returned by the fe-core cache registry; connectors interact with their
 * caches exclusively through this interface.</p>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface MetaCacheHandle<K, V> {

    /** Returns the value for {@code key} if cached, otherwise {@link Optional#empty()}. */
    Optional<V> getIfPresent(K key);

    /**
     * Loads via the binding's {@link CacheLoader} if absent. May throw
     * {@link RuntimeException} wrapping the loader's checked exception.
     */
    V get(K key);

    /** Stores the given mapping in the cache. */
    void put(K key, V value);

    /** Invalidate a single key. */
    void invalidate(K key);

    /** Invalidate every entry. */
    void invalidateAll();

    /** Snapshot of the cache statistics. */
    CacheStats stats();

    /**
     * Snapshot of cache stats. Mirrors Caffeine's {@code CacheStats} but
     * does not depend on Caffeine.
     */
    final class CacheStats {
        private final long hitCount;
        private final long missCount;
        private final long loadSuccessCount;
        private final long loadFailureCount;
        private final long totalLoadTimeNanos;
        private final long evictionCount;
        private final long currentSize;

        public CacheStats(long hitCount,
                          long missCount,
                          long loadSuccessCount,
                          long loadFailureCount,
                          long totalLoadTimeNanos,
                          long evictionCount,
                          long currentSize) {
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.loadSuccessCount = loadSuccessCount;
            this.loadFailureCount = loadFailureCount;
            this.totalLoadTimeNanos = totalLoadTimeNanos;
            this.evictionCount = evictionCount;
            this.currentSize = currentSize;
        }

        /** Returns a stats snapshot whose counters are all zero. */
        public static CacheStats empty() {
            return new CacheStats(0L, 0L, 0L, 0L, 0L, 0L, 0L);
        }

        public long getHitCount() {
            return hitCount;
        }

        public long getMissCount() {
            return missCount;
        }

        public long getLoadSuccessCount() {
            return loadSuccessCount;
        }

        public long getLoadFailureCount() {
            return loadFailureCount;
        }

        public long getTotalLoadTimeNanos() {
            return totalLoadTimeNanos;
        }

        public long getEvictionCount() {
            return evictionCount;
        }

        public long getCurrentSize() {
            return currentSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheStats)) {
                return false;
            }
            CacheStats that = (CacheStats) o;
            return hitCount == that.hitCount
                    && missCount == that.missCount
                    && loadSuccessCount == that.loadSuccessCount
                    && loadFailureCount == that.loadFailureCount
                    && totalLoadTimeNanos == that.totalLoadTimeNanos
                    && evictionCount == that.evictionCount
                    && currentSize == that.currentSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hitCount, missCount, loadSuccessCount, loadFailureCount,
                    totalLoadTimeNanos, evictionCount, currentSize);
        }

        @Override
        public String toString() {
            return "CacheStats{hitCount=" + hitCount
                    + ", missCount=" + missCount
                    + ", loadSuccessCount=" + loadSuccessCount
                    + ", loadFailureCount=" + loadFailureCount
                    + ", totalLoadTimeNanos=" + totalLoadTimeNanos
                    + ", evictionCount=" + evictionCount
                    + ", currentSize=" + currentSize
                    + '}';
        }
    }
}
