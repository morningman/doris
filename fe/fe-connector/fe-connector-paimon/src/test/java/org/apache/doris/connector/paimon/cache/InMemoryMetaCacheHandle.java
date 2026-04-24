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

package org.apache.doris.connector.paimon.cache;

import org.apache.doris.connector.api.cache.CacheLoader;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.cache.RemovalListener;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal in-memory {@link MetaCacheHandle} used by paimon cache tests.
 * Mirrors the fe-core Caffeine-backed handle's user-visible contract
 * (load on miss, invalidate, stats counters) without pulling fe-core into
 * the plugin's test classpath. Also fires a registered
 * {@link RemovalListener} on explicit and bulk invalidations so tests can
 * assert close-on-removal semantics.
 */
final class InMemoryMetaCacheHandle<K, V> implements MetaCacheHandle<K, V> {

    private final ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>();
    private final CacheLoader<K, V> loader;
    private final RemovalListener<K, V> removalListener;
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong loadSuccess = new AtomicLong();
    private final AtomicLong loadFailure = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    InMemoryMetaCacheHandle(CacheLoader<K, V> loader, RemovalListener<K, V> removalListener) {
        this.loader = Objects.requireNonNull(loader, "loader");
        this.removalListener = removalListener;
    }

    @Override
    public Optional<V> getIfPresent(K key) {
        V v = map.get(key);
        if (v != null) {
            hits.incrementAndGet();
        }
        return Optional.ofNullable(v);
    }

    @Override
    public V get(K key) {
        V cached = map.get(key);
        if (cached != null) {
            hits.incrementAndGet();
            return cached;
        }
        misses.incrementAndGet();
        try {
            V loaded = loader.load(key);
            map.put(key, loaded);
            loadSuccess.incrementAndGet();
            return loaded;
        } catch (Exception e) {
            loadFailure.incrementAndGet();
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(K key, V value) {
        V old = map.put(key, value);
        if (old != null) {
            fire(key, old, RemovalListener.Cause.REPLACED);
        }
    }

    @Override
    public void invalidate(K key) {
        V removed = map.remove(key);
        if (removed != null) {
            evictions.incrementAndGet();
            fire(key, removed, RemovalListener.Cause.EXPLICIT);
        }
    }

    @Override
    public void invalidateAll() {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            evictions.incrementAndGet();
            fire(entry.getKey(), entry.getValue(), RemovalListener.Cause.EXPLICIT);
        }
        map.clear();
    }

    @Override
    public CacheStats stats() {
        return new CacheStats(hits.get(), misses.get(), loadSuccess.get(), loadFailure.get(),
                0L, evictions.get(), map.size());
    }

    int sizeForTest() {
        return map.size();
    }

    private void fire(K key, V value, RemovalListener.Cause cause) {
        if (removalListener != null) {
            removalListener.onRemoval(key, value, cause);
        }
    }
}
