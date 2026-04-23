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

package org.apache.doris.connector.cache;

import org.apache.doris.connector.api.cache.ConnectorCacheSpec;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.api.cache.RemovalListener;
import org.apache.doris.connector.api.cache.Weigher;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import java.util.Optional;
import java.util.concurrent.CompletionException;

/**
 * Caffeine-backed implementation of {@link MetaCacheHandle}. Package-private
 * because construction is owned by {@link ConnectorMetaCacheRegistry}.
 *
 * <p>Honored spec fields: {@code maxSize} (as {@code maximumSize} or
 * {@code maximumWeight} when a {@link Weigher} is present), {@code ttl} (as
 * {@code expireAfterWrite}), {@code refreshAfter} (as
 * {@code refreshAfterWrite}), {@code softValues}, and {@link RemovalListener}.
 * The {@link ConnectorCacheSpec#getRefreshPolicy()} enum is read but currently
 * treated as TTL; {@code ALWAYS_REFRESH} / {@code MANUAL_ONLY} differentiation
 * is deferred to a later PR.</p>
 */
final class CaffeineMetaCacheHandle<K, V> implements MetaCacheHandle<K, V> {

    private final ConnectorMetaCacheBinding<K, V> binding;
    private final LoadingCache<K, V> cache;

    private CaffeineMetaCacheHandle(ConnectorMetaCacheBinding<K, V> binding, LoadingCache<K, V> cache) {
        this.binding = binding;
        this.cache = cache;
    }

    ConnectorMetaCacheBinding<K, V> getBinding() {
        return binding;
    }

    static <K, V> CaffeineMetaCacheHandle<K, V> build(ConnectorMetaCacheBinding<K, V> binding) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats();

        ConnectorCacheSpec spec = binding.getDefaultSpec();

        Optional<Weigher<K, V>> weigherOpt = binding.getWeigher();
        if (spec.getMaxSize() > 0) {
            if (weigherOpt.isPresent()) {
                Weigher<K, V> w = weigherOpt.get();
                builder = builder
                        .maximumWeight(spec.getMaxSize())
                        .weigher((Object k, Object v) -> {
                            @SuppressWarnings("unchecked")
                            int weight = w.weigh((K) k, (V) v);
                            return Math.max(0, weight);
                        });
            } else {
                builder = builder.maximumSize(spec.getMaxSize());
            }
        }

        if (spec.getTtl() != null) {
            builder = builder.expireAfterWrite(spec.getTtl());
        }

        if (spec.getRefreshAfter() != null) {
            builder = builder.refreshAfterWrite(spec.getRefreshAfter());
        }

        if (spec.isSoftValues()) {
            builder = builder.softValues();
        }

        Optional<RemovalListener<K, V>> removalOpt = binding.getRemovalListener();
        if (removalOpt.isPresent()) {
            RemovalListener<K, V> apiListener = removalOpt.get();
            builder = builder.removalListener((Object key, Object value, RemovalCause cause) -> {
                if (key == null || value == null) {
                    return;
                }
                @SuppressWarnings("unchecked")
                K k = (K) key;
                @SuppressWarnings("unchecked")
                V v = (V) value;
                apiListener.onRemoval(k, v, toApiCause(cause));
            });
        }

        org.apache.doris.connector.api.cache.CacheLoader<K, V> apiLoader = binding.getLoader();
        com.github.benmanes.caffeine.cache.CacheLoader<K, V> caffeineLoader = key -> {
            try {
                return apiLoader.load(key);
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        };

        LoadingCache<K, V> cache = builder.build(caffeineLoader);
        return new CaffeineMetaCacheHandle<>(binding, cache);
    }

    private static RemovalListener.Cause toApiCause(RemovalCause cause) {
        switch (cause) {
            case EXPLICIT:
                return RemovalListener.Cause.EXPLICIT;
            case REPLACED:
                return RemovalListener.Cause.REPLACED;
            case SIZE:
                return RemovalListener.Cause.SIZE;
            case EXPIRED:
            case COLLECTED:
            default:
                return RemovalListener.Cause.EXPIRED;
        }
    }

    @Override
    public Optional<V> getIfPresent(K key) {
        return Optional.ofNullable(cache.getIfPresent(key));
    }

    @Override
    public V get(K key) {
        try {
            return cache.get(key);
        } catch (CompletionException ce) {
            Throwable c = ce.getCause();
            if (c instanceof RuntimeException) {
                throw (RuntimeException) c;
            }
            if (c != null) {
                throw new RuntimeException(c);
            }
            throw ce;
        }
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public CacheStats stats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats s = cache.stats();
        cache.cleanUp();
        return new CacheStats(
                s.hitCount(),
                s.missCount(),
                s.loadSuccessCount(),
                s.loadFailureCount(),
                s.totalLoadTime(),
                s.evictionCount(),
                cache.estimatedSize());
    }
}
