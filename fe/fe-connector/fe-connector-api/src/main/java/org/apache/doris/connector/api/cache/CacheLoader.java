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

/**
 * Loads a value for a given key. Pure SPI type so it does not depend on
 * Caffeine or Guava; the fe-core registry adapts this to the underlying
 * cache implementation.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@FunctionalInterface
public interface CacheLoader<K, V> {
    /**
     * Load the value associated with {@code key}.
     *
     * @param key the cache key (never {@code null})
     * @return the loaded value
     * @throws Exception if loading fails; the registry adapter is responsible
     *         for surfacing failures via {@link MetaCacheHandle#get(Object)}
     */
    V load(K key) throws Exception;
}
