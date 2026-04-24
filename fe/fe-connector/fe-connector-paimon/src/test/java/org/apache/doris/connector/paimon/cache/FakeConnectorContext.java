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

import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;
import org.apache.doris.connector.spi.ConnectorContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory {@link ConnectorContext} test double that wires connector
 * bindings to the package-private {@link InMemoryMetaCacheHandle}. Mirrors
 * the contract of fe-core's {@code DefaultConnectorContext} for the subset
 * exercised by paimon plugin cache tests.
 */
final class FakeConnectorContext implements ConnectorContext {

    private final String catalogName;
    private final long catalogId;
    private final Map<String, InMemoryMetaCacheHandle<?, ?>> handles = new LinkedHashMap<>();
    private int invalidateCount;
    private InvalidateRequest lastRequest;

    FakeConnectorContext(String catalogName, long catalogId) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.catalogId = catalogId;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public long getCatalogId() {
        return catalogId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> MetaCacheHandle<K, V> getOrCreateCache(ConnectorMetaCacheBinding<K, V> binding) {
        Objects.requireNonNull(binding, "binding");
        InMemoryMetaCacheHandle<?, ?> existing = handles.get(binding.getEntryName());
        if (existing != null) {
            return (MetaCacheHandle<K, V>) existing;
        }
        InMemoryMetaCacheHandle<K, V> fresh = new InMemoryMetaCacheHandle<>(
                binding.getLoader(), binding.getRemovalListener().orElse(null));
        handles.put(binding.getEntryName(), fresh);
        return fresh;
    }

    @Override
    public void invalidate(InvalidateRequest req) {
        this.invalidateCount++;
        this.lastRequest = req;
        for (InMemoryMetaCacheHandle<?, ?> h : handles.values()) {
            h.invalidateAll();
        }
    }

    @Override
    public void invalidateAll() {
        for (InMemoryMetaCacheHandle<?, ?> h : handles.values()) {
            h.invalidateAll();
        }
    }

    InMemoryMetaCacheHandle<?, ?> handleFor(String entryName) {
        return handles.get(entryName);
    }

    int registeredHandleCount() {
        return handles.size();
    }

    int invalidateCallCount() {
        return invalidateCount;
    }

    InvalidateRequest lastInvalidateRequest() {
        return lastRequest;
    }
}
