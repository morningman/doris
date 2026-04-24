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

import org.apache.doris.connector.api.cache.CacheSnapshot;
import org.apache.doris.connector.api.cache.ConnectorMetaCacheBinding;
import org.apache.doris.connector.api.cache.InvalidateRequest;
import org.apache.doris.connector.api.cache.MetaCacheHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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

    /**
     * Explicit binding entry point used by the engine when wiring a plugin
     * connector's {@code getMetaCacheBindings()} declarations into this
     * context. Equivalent to {@link #getOrCreateCache(ConnectorMetaCacheBinding)}
     * but documents intent: the caller is performing one-time registration at
     * catalog initialization time, not opportunistic on-demand creation.
     *
     * @return the {@link MetaCacheHandle} owned by this registry for the
     *         given binding.
     */
    public <K, V> MetaCacheHandle<K, V> bind(ConnectorMetaCacheBinding<K, V> binding) {
        return getOrCreateCache(binding);
    }

    /**
     * Bind every binding in {@code bindings}. Null entries are rejected.
     * Equivalent to calling {@link #bind(ConnectorMetaCacheBinding)} for each
     * element. Returns the number of NEWLY-registered bindings (existing
     * bindings with the same {@code entryName} that already match are not
     * counted).
     */
    public int bindAll(Iterable<? extends ConnectorMetaCacheBinding<?, ?>> bindings) {
        Objects.requireNonNull(bindings, "bindings");
        int created = 0;
        for (ConnectorMetaCacheBinding<?, ?> b : bindings) {
            if (b == null) {
                throw new IllegalArgumentException("bindings must not contain null entries");
            }
            int before = registrations.size();
            bindWildcard(b);
            if (registrations.size() > before) {
                created++;
            }
        }
        return created;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void bindWildcard(ConnectorMetaCacheBinding<?, ?> b) {
        getOrCreateCache((ConnectorMetaCacheBinding) b);
    }

    /** Snapshot of every binding's stats; safe to call concurrently with bind/invalidate. */
    public List<CacheSnapshot> snapshot() {
        List<CacheSnapshot> out = new ArrayList<>(registrations.size());
        for (Registration<?, ?> reg : registrations.values()) {
            out.add(new CacheSnapshot(reg.binding.getEntryName(), reg.handle.stats()));
        }
        return out;
    }

    /** Immutable snapshot of currently registered entry names. */
    public Set<String> entryNames() {
        return Collections.unmodifiableSet(registrations.keySet());
    }

    /**
     * Tear down every binding owned by this registry: invalidates each cache
     * and removes it from the registration map. Subsequent {@link #size()}
     * returns zero. Intended to be called from the catalog's {@code onClose()}
     * lifecycle hook so that a stale registry does not outlive its connector.
     *
     * <p>This method is idempotent — calling it twice is a no-op.</p>
     */
    public void detach() {
        for (Registration<?, ?> reg : registrations.values()) {
            reg.handle.invalidateAll();
        }
        registrations.clear();
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
