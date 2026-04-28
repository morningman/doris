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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.mtmv.MTMVPluginMvccSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public class PluginMtmvSnapshotBridgeTest {

    @Test
    public void toMtmvAndBackRoundTrip() {
        ConnectorProvider provider = new TokenCodecProvider("iceberg-fake");
        ConnectorMvccSnapshot original = new TokenSnapshot("token-99", Instant.ofEpochSecond(1700000000L));

        Optional<MTMVPluginMvccSnapshot> persisted = PluginMtmvSnapshotBridge.toMtmv(provider, original);
        Assertions.assertTrue(persisted.isPresent());
        Assertions.assertEquals("iceberg-fake", persisted.get().getConnectorType());
        Assertions.assertFalse(persisted.get().getBase64Payload().isEmpty());

        Optional<ConnectorMvccSnapshot> decoded = PluginMtmvSnapshotBridge.fromMtmv(provider, persisted.get());
        Assertions.assertTrue(decoded.isPresent());
        Assertions.assertEquals(original.toOpaqueToken(), decoded.get().toOpaqueToken());
    }

    @Test
    public void absentCodecReturnsEmptyBothDirections() {
        ConnectorProvider provider = new NoCodecProvider();
        Optional<MTMVPluginMvccSnapshot> persisted = PluginMtmvSnapshotBridge.toMtmv(
                provider, new TokenSnapshot("x", Instant.EPOCH));
        Assertions.assertTrue(persisted.isEmpty());

        // A snapshot persisted by a different (codec-bearing) provider must
        // still come back empty when the current provider has no codec.
        MTMVPluginMvccSnapshot stub = new MTMVPluginMvccSnapshot("noop", "AAAA");
        Assertions.assertTrue(PluginMtmvSnapshotBridge.fromMtmv(provider, stub).isEmpty());
    }

    @Test
    public void connectorTypeMismatchIsRejected() {
        ConnectorProvider provider = new TokenCodecProvider("iceberg-fake");
        MTMVPluginMvccSnapshot foreign = new MTMVPluginMvccSnapshot("paimon-fake", "AAAA");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PluginMtmvSnapshotBridge.fromMtmv(provider, foreign));
    }

    @Test
    public void connectorTypeMatchIsCaseInsensitive() {
        ConnectorProvider provider = new TokenCodecProvider("Iceberg-Fake");
        MTMVPluginMvccSnapshot persisted = new MTMVPluginMvccSnapshot("iceberg-fake",
                java.util.Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.UTF_8)));
        Optional<ConnectorMvccSnapshot> decoded = PluginMtmvSnapshotBridge.fromMtmv(provider, persisted);
        Assertions.assertTrue(decoded.isPresent());
        Assertions.assertEquals("hello", decoded.get().toOpaqueToken());
    }

    @Test
    public void snapshotEqualsAndHashCode() {
        MTMVPluginMvccSnapshot a = new MTMVPluginMvccSnapshot("iceberg-fake", "AAAA");
        MTMVPluginMvccSnapshot b = new MTMVPluginMvccSnapshot("iceberg-fake", "AAAA");
        MTMVPluginMvccSnapshot diffPayload = new MTMVPluginMvccSnapshot("iceberg-fake", "BBBB");
        MTMVPluginMvccSnapshot diffType = new MTMVPluginMvccSnapshot("paimon-fake", "AAAA");

        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, diffPayload);
        Assertions.assertNotEquals(a, diffType);
    }

    @Test
    public void gsonRoundTripPreservesPayloadAndType() {
        MTMVPluginMvccSnapshot original = new MTMVPluginMvccSnapshot(
                "iceberg-fake",
                java.util.Base64.getEncoder().encodeToString(new byte[]{1, 2, 3, 4, 5}));

        // Serialise through the polymorphic MTMVSnapshotIf adapter so we
        // exercise the GsonUtils registration path the refresh records use.
        String json = GsonUtils.GSON.toJson(original, MTMVSnapshotIf.class);
        Assertions.assertTrue(json.contains("MTMVPluginMvccSnapshot"));

        MTMVSnapshotIf restored = GsonUtils.GSON.fromJson(json, MTMVSnapshotIf.class);
        Assertions.assertTrue(restored instanceof MTMVPluginMvccSnapshot);
        Assertions.assertEquals(original, restored);
    }

    // --- test fixtures ---

    private static final class TokenSnapshot implements ConnectorMvccSnapshot {
        private final String token;
        private final Instant commit;

        TokenSnapshot(String token, Instant commit) {
            this.token = token;
            this.commit = commit;
        }

        @Override
        public Instant commitTime() {
            return commit;
        }

        @Override
        public Optional<ConnectorTableVersion> asVersion() {
            return Optional.empty();
        }

        @Override
        public String toOpaqueToken() {
            return token;
        }
    }

    private static class TokenCodecProvider implements ConnectorProvider {
        private final String type;

        TokenCodecProvider(String type) {
            this.type = type;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            throw new UnsupportedOperationException("not used by bridge");
        }

        @Override
        public Optional<ConnectorMvccSnapshot.Codec> getMvccSnapshotCodec() {
            return Optional.of(new ConnectorMvccSnapshot.Codec() {
                @Override
                public byte[] encode(ConnectorMvccSnapshot s) {
                    return s.toOpaqueToken().getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public ConnectorMvccSnapshot decode(byte[] bytes) {
                    return new TokenSnapshot(new String(bytes, StandardCharsets.UTF_8), Instant.EPOCH);
                }
            });
        }
    }

    private static final class NoCodecProvider implements ConnectorProvider {
        @Override
        public String getType() {
            return "noop";
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            throw new UnsupportedOperationException("not used by bridge");
        }
    }
}
