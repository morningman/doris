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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public class MvccSnapshotPersistenceTest {

    /**
     * Round-trips a fixed-payload snapshot through a codec that simply
     * encodes the opaque token as UTF-8 (mirrors the iceberg codec
     * contract: bytes carry their own framing).
     */
    @Test
    public void serializeDeserializeRoundTrip() {
        ConnectorProvider provider = new TokenCodecProvider();
        ConnectorMvccSnapshot original = new TokenSnapshot("token-42", Instant.ofEpochSecond(1700000000L));

        Optional<String> base64 = MvccSnapshotPersistence.serialize(provider, original);
        Assertions.assertTrue(base64.isPresent());
        Assertions.assertFalse(base64.get().isEmpty());

        Optional<ConnectorMvccSnapshot> decoded = MvccSnapshotPersistence.deserialize(provider, base64.get());
        Assertions.assertTrue(decoded.isPresent());
        Assertions.assertEquals(original.toOpaqueToken(), decoded.get().toOpaqueToken());
    }

    @Test
    public void absentCodecSerializeReturnsEmpty() {
        ConnectorProvider provider = new NoCodecProvider();
        Optional<String> base64 = MvccSnapshotPersistence.serialize(
                provider, new TokenSnapshot("x", Instant.EPOCH));
        Assertions.assertTrue(base64.isEmpty());
    }

    @Test
    public void absentCodecDeserializeReturnsEmpty() {
        ConnectorProvider provider = new NoCodecProvider();
        Optional<ConnectorMvccSnapshot> decoded =
                MvccSnapshotPersistence.deserialize(provider, "AAAA");
        Assertions.assertTrue(decoded.isEmpty());
    }

    @Test
    public void deserializeRejectsInvalidBase64() {
        ConnectorProvider provider = new TokenCodecProvider();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MvccSnapshotPersistence.deserialize(provider, "not base64!!!"));
    }

    @Test
    public void serializedFormIsAsciiBase64() {
        ConnectorProvider provider = new TokenCodecProvider();
        Optional<String> base64 = MvccSnapshotPersistence.serialize(
                provider, new TokenSnapshot("ascii-payload", Instant.EPOCH));
        Assertions.assertTrue(base64.isPresent());
        // Base64 alphabet only.
        Assertions.assertTrue(base64.get().matches("[A-Za-z0-9+/]*={0,2}"));
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

    private static final class TokenCodecProvider extends BaseProvider {
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

    private static final class NoCodecProvider extends BaseProvider {
    }

    private abstract static class BaseProvider implements ConnectorProvider {
        @Override
        public String getType() {
            return "test-only";
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
            throw new UnsupportedOperationException("not used by persistence helper");
        }
    }
}
