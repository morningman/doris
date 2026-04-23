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

/**
 * Pairs a binding name with a {@link MetaCacheHandle.CacheStats} snapshot,
 * used by {@code Connector#listSelfManagedCacheStats()} to expose plugin-owned
 * cache metrics without coupling the {@link
 * org.apache.doris.connector.api.Connector} surface to {@link MetaCacheHandle}.
 */
public final class CacheSnapshot {

    private final String bindingName;
    private final MetaCacheHandle.CacheStats stats;

    public CacheSnapshot(String bindingName, MetaCacheHandle.CacheStats stats) {
        if (bindingName == null || bindingName.isEmpty()) {
            throw new IllegalArgumentException("bindingName must not be null or empty");
        }
        if (stats == null) {
            throw new IllegalArgumentException("stats must not be null");
        }
        this.bindingName = bindingName;
        this.stats = stats;
    }

    public String getBindingName() {
        return bindingName;
    }

    public MetaCacheHandle.CacheStats getStats() {
        return stats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CacheSnapshot)) {
            return false;
        }
        CacheSnapshot that = (CacheSnapshot) o;
        return Objects.equals(bindingName, that.bindingName)
                && Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bindingName, stats);
    }

    @Override
    public String toString() {
        return "CacheSnapshot{bindingName=" + bindingName + ", stats=" + stats + '}';
    }
}
