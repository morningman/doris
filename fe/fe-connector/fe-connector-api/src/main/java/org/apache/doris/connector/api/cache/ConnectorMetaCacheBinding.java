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
 * Immutable description of a cache binding declared by a connector and
 * managed by the fe-core cache registry.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class ConnectorMetaCacheBinding<K, V> {

    private final String entryName;
    private final Class<K> keyType;
    private final Class<V> valueType;
    private final CacheLoader<K, V> loader;
    private final ConnectorCacheSpec defaultSpec;
    private final ConnectorMetaCacheInvalidation invalidationStrategy;
    private final Weigher<K, V> weigher;
    private final RemovalListener<K, V> removalListener;

    private ConnectorMetaCacheBinding(Builder<K, V> b) {
        if (b.entryName == null || b.entryName.isEmpty()) {
            throw new IllegalArgumentException("entryName must not be null or empty");
        }
        if (b.keyType == null) {
            throw new IllegalArgumentException("keyType must not be null");
        }
        if (b.valueType == null) {
            throw new IllegalArgumentException("valueType must not be null");
        }
        if (b.loader == null) {
            throw new IllegalArgumentException("loader must not be null");
        }
        this.entryName = b.entryName;
        this.keyType = b.keyType;
        this.valueType = b.valueType;
        this.loader = b.loader;
        this.defaultSpec = b.defaultSpec != null ? b.defaultSpec : ConnectorCacheSpec.defaults();
        this.invalidationStrategy = b.invalidationStrategy != null
                ? b.invalidationStrategy
                : ConnectorMetaCacheInvalidation.always();
        this.weigher = b.weigher;
        this.removalListener = b.removalListener;
    }

    public static <K, V> Builder<K, V> builder(String entryName,
                                               Class<K> keyType,
                                               Class<V> valueType,
                                               CacheLoader<K, V> loader) {
        return new Builder<>(entryName, keyType, valueType, loader);
    }

    public String getEntryName() {
        return entryName;
    }

    public Class<K> getKeyType() {
        return keyType;
    }

    public Class<V> getValueType() {
        return valueType;
    }

    public CacheLoader<K, V> getLoader() {
        return loader;
    }

    public ConnectorCacheSpec getDefaultSpec() {
        return defaultSpec;
    }

    public ConnectorMetaCacheInvalidation getInvalidationStrategy() {
        return invalidationStrategy;
    }

    public Optional<Weigher<K, V>> getWeigher() {
        return Optional.ofNullable(weigher);
    }

    public Optional<RemovalListener<K, V>> getRemovalListener() {
        return Optional.ofNullable(removalListener);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorMetaCacheBinding)) {
            return false;
        }
        ConnectorMetaCacheBinding<?, ?> that = (ConnectorMetaCacheBinding<?, ?>) o;
        return Objects.equals(entryName, that.entryName)
                && Objects.equals(keyType, that.keyType)
                && Objects.equals(valueType, that.valueType)
                && Objects.equals(loader, that.loader)
                && Objects.equals(defaultSpec, that.defaultSpec)
                && Objects.equals(invalidationStrategy, that.invalidationStrategy)
                && Objects.equals(weigher, that.weigher)
                && Objects.equals(removalListener, that.removalListener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entryName, keyType, valueType, loader, defaultSpec,
                invalidationStrategy, weigher, removalListener);
    }

    @Override
    public String toString() {
        return "ConnectorMetaCacheBinding{entryName=" + entryName
                + ", keyType=" + keyType
                + ", valueType=" + valueType
                + ", defaultSpec=" + defaultSpec
                + ", hasWeigher=" + (weigher != null)
                + ", hasRemovalListener=" + (removalListener != null)
                + '}';
    }

    /** Fluent builder for {@link ConnectorMetaCacheBinding}. */
    public static final class Builder<K, V> {
        private final String entryName;
        private final Class<K> keyType;
        private final Class<V> valueType;
        private final CacheLoader<K, V> loader;
        private ConnectorCacheSpec defaultSpec;
        private ConnectorMetaCacheInvalidation invalidationStrategy;
        private Weigher<K, V> weigher;
        private RemovalListener<K, V> removalListener;

        private Builder(String entryName, Class<K> keyType, Class<V> valueType, CacheLoader<K, V> loader) {
            this.entryName = entryName;
            this.keyType = keyType;
            this.valueType = valueType;
            this.loader = loader;
        }

        public Builder<K, V> defaultSpec(ConnectorCacheSpec defaultSpec) {
            this.defaultSpec = defaultSpec;
            return this;
        }

        public Builder<K, V> invalidationStrategy(ConnectorMetaCacheInvalidation invalidationStrategy) {
            this.invalidationStrategy = invalidationStrategy;
            return this;
        }

        public Builder<K, V> weigher(Weigher<K, V> weigher) {
            this.weigher = weigher;
            return this;
        }

        public Builder<K, V> removalListener(RemovalListener<K, V> removalListener) {
            this.removalListener = removalListener;
            return this;
        }

        public ConnectorMetaCacheBinding<K, V> build() {
            return new ConnectorMetaCacheBinding<>(this);
        }
    }
}
