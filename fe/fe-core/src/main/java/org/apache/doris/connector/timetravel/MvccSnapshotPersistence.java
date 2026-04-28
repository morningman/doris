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

package org.apache.doris.connector.timetravel;

import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.ConnectorProvider;

import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * Helper for round-tripping a {@link ConnectorMvccSnapshot} through the
 * plugin-provided {@link ConnectorMvccSnapshot.Codec} into a Base64 ASCII
 * blob suitable for persistence in MTMV refresh metadata (and any other
 * engine state machine that needs an opaque, URL/JSON-safe form).
 *
 * <p>The codec is sourced from {@link ConnectorProvider#getMvccSnapshotCodec()};
 * connectors that do not declare
 * {@link org.apache.doris.connector.api.ConnectorCapability#SUPPORTS_MVCC_SNAPSHOT}
 * keep the empty default and both {@link #serialize}/{@link #deserialize}
 * return {@link Optional#empty()} (no exception). This matches the
 * D5 §3.3 contract: the engine never asks for a codec it was not promised
 * one for.</p>
 *
 * <p>Magic + version framing inside the byte stream is the codec's
 * responsibility (see {@code IcebergConnectorMvccSnapshot.Codec}); this
 * helper layers Base64 (RFC 4648, no padding stripping) on top so the
 * serialised form is safe to embed in JSON / SQL state.</p>
 *
 * <p>The MTMV refresh layer consumes this helper through
 * {@link PluginMtmvSnapshotBridge}, which adds a connector-type tag and
 * exposes the result as the GSON-persistable
 * {@link org.apache.doris.mtmv.MTMVPluginMvccSnapshot}. The legacy
 * iceberg/hudi/paimon paths continue to bridge through their own
 * {@code MvccSnapshot} subclasses; that cutover is tracked separately
 * (M3-iceberg-cutover et al.).</p>
 */
public final class MvccSnapshotPersistence {

    private MvccSnapshotPersistence() {
    }

    /**
     * Serialise {@code snapshot} via {@code provider}'s codec into Base64.
     *
     * @return the Base64-encoded codec bytes, or {@link Optional#empty()}
     *         when the provider exposes no codec
     */
    public static Optional<String> serialize(ConnectorProvider provider,
                                             ConnectorMvccSnapshot snapshot) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(snapshot, "snapshot");
        Optional<ConnectorMvccSnapshot.Codec> codec = provider.getMvccSnapshotCodec();
        if (codec.isEmpty()) {
            return Optional.empty();
        }
        byte[] bytes = codec.get().encode(snapshot);
        Objects.requireNonNull(bytes, "codec.encode returned null");
        return Optional.of(Base64.getEncoder().encodeToString(bytes));
    }

    /**
     * Deserialise a Base64 blob via {@code provider}'s codec.
     *
     * @return the decoded snapshot, or {@link Optional#empty()} when the
     *         provider exposes no codec
     * @throws IllegalArgumentException if {@code base64} is not valid Base64
     */
    public static Optional<ConnectorMvccSnapshot> deserialize(ConnectorProvider provider,
                                                              String base64) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(base64, "base64");
        Optional<ConnectorMvccSnapshot.Codec> codec = provider.getMvccSnapshotCodec();
        if (codec.isEmpty()) {
            return Optional.empty();
        }
        byte[] bytes = Base64.getDecoder().decode(base64);
        ConnectorMvccSnapshot decoded = codec.get().decode(bytes);
        Objects.requireNonNull(decoded, "codec.decode returned null");
        return Optional.of(decoded);
    }
}
