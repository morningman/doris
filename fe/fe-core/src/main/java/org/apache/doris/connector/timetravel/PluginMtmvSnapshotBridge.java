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
import org.apache.doris.mtmv.MTMVPluginMvccSnapshot;

import java.util.Objects;
import java.util.Optional;

/**
 * Bridge that round-trips a {@link ConnectorMvccSnapshot} between the SPI
 * surface and the MTMV refresh persistence value type
 * {@link MTMVPluginMvccSnapshot}.
 *
 * <p>This is the consumer of {@link MvccSnapshotPersistence} on the MTMV
 * side: it layers the connector-type tag over the Base64 payload so the
 * persisted record knows which plugin's codec must decode it. Decode-time
 * mismatch (a snapshot tagged for connector A handed to provider B) is
 * rejected eagerly so a misrouted refresh fails loud rather than silently
 * decoding nonsense bytes.</p>
 *
 * <p>When a connector has no codec
 * ({@link ConnectorProvider#getMvccSnapshotCodec()} empty), both directions
 * return {@link Optional#empty()} — matching the D5 §3.3 contract that the
 * engine never asks for a codec it was not promised one for.</p>
 */
public final class PluginMtmvSnapshotBridge {

    private PluginMtmvSnapshotBridge() {
    }

    /**
     * Encode a connector-side snapshot into the MTMV value type.
     *
     * @return the persistable {@link MTMVPluginMvccSnapshot}, or
     *         {@link Optional#empty()} when {@code provider} exposes no codec
     */
    public static Optional<MTMVPluginMvccSnapshot> toMtmv(ConnectorProvider provider,
                                                          ConnectorMvccSnapshot snapshot) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(snapshot, "snapshot");
        Optional<String> base64 = MvccSnapshotPersistence.serialize(provider, snapshot);
        if (base64.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new MTMVPluginMvccSnapshot(provider.getType(), base64.get()));
    }

    /**
     * Decode a persisted MTMV snapshot back to the connector-side type.
     *
     * @return the decoded {@link ConnectorMvccSnapshot}, or
     *         {@link Optional#empty()} when {@code provider} exposes no codec
     * @throws IllegalArgumentException if the persisted snapshot's connector
     *         type tag does not match {@code provider.getType()}, or if the
     *         payload is not valid Base64
     */
    public static Optional<ConnectorMvccSnapshot> fromMtmv(ConnectorProvider provider,
                                                           MTMVPluginMvccSnapshot persisted) {
        Objects.requireNonNull(provider, "provider");
        Objects.requireNonNull(persisted, "persisted");
        String expected = provider.getType();
        String actual = persisted.getConnectorType();
        if (!expected.equalsIgnoreCase(actual)) {
            throw new IllegalArgumentException(
                    "MTMVPluginMvccSnapshot connector-type mismatch: persisted=" + actual
                            + ", provider=" + expected);
        }
        return MvccSnapshotPersistence.deserialize(provider, persisted.getBase64Payload());
    }
}
