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

import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-context registry that maps each {@link ConnectorMetaCacheBinding} to a
 * real Caffeine-backed {@link MetaCacheHandle}. Lives inside a
 * {@code DefaultConnectorContext} and is the fe-core implementation that
 * replaces the SPI's {@code getOrCreateCache} default (which throws UOE).
 *
 * <p>This is the M0 skeleton: it wires registration, same-binding dedup, and
 * bulk invalidation dispatch. Key-selective invalidation (e.g. invalidate a
 * specific {@code (db, table)} entry from an {@link InvalidateRequest}) needs
 * a key-adapter that the binding does not yet expose — deferred.</p>
 */
public final class ConnectorMetaCacheRegistry {

    private final ConcurrentHashMap<String, Registration<?, ?>> registrations = new ConcurrentHashMap<>();
    private final String catalogName;

    public ConnectorMetaCacheRegistry(String catalogName) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    }

    public String getCatalogName() {
        return catalogName;
    }

    /**
     * Return the {@link MetaCacheHandle} backing the given binding, creating
     * (and caching) the Caffeine instance on first call. Subsequent calls with
     * the same binding instance return the same handle.
     *
     * @throws IllegalStateException if a different binding has already been
     *         registered under the same {@code entryName} (one binding per
     *         entryName per context).
     */
    @SuppressWarnings("unchecked")
    public <K, V> MetaCacheHandle<K, V> getOrCreateCache(ConnectorMetaCacheBinding<K, V> binding) {
        Objects.requireNonNull(binding, "binding");
        Registration<?, ?> existing = registrations.computeIfAbsent(
                binding.getEntryName(), name -> createRegistration(binding));
        if (!existing.binding.equals(binding)) {
            throw new IllegalStateException(
                    "duplicate cache binding for entryName=" + binding.getEntryName()
                            + " with different spec in catalog=" + catalogName);
        }
        return (MetaCacheHandle<K, V>) existing.handle;
    }

    /**
     * Dispatch an {@link InvalidateRequest} to every binding whose
     * {@code invalidationStrategy} matches. Currently performs bulk
     * {@code invalidateAll()} on matching bindings; key-selective dispatch is
     * a follow-up.
     */
    public void invalidate(InvalidateRequest req) {
        Objects.requireNonNull(req, "req");
        for (Registration<?, ?> reg : registrations.values()) {
            if (reg.binding.getInvalidationStrategy().appliesTo(req)) {
                reg.handle.invalidateAll();
            }
        }
    }

    /** Invalidate every binding in this context. */
    public void invalidateAll() {
        for (Registration<?, ?> reg : registrations.values()) {
            reg.handle.invalidateAll();
        }
    }

    public int size() {
        return registrations.size();
    }

    public boolean contains(String entryName) {
        return registrations.containsKey(entryName);
    }

    private <K, V> Registration<K, V> createRegistration(ConnectorMetaCacheBinding<K, V> binding) {
        CaffeineMetaCacheHandle<K, V> handle = CaffeineMetaCacheHandle.build(binding);
        return new Registration<>(binding, handle);
    }

    private static final class Registration<K, V> {
        final ConnectorMetaCacheBinding<K, V> binding;
        final CaffeineMetaCacheHandle<K, V> handle;

        Registration(ConnectorMetaCacheBinding<K, V> binding, CaffeineMetaCacheHandle<K, V> handle) {
            this.binding = binding;
            this.handle = handle;
        }
    }
}
