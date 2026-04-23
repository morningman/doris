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

package org.apache.doris.connector.api.timetravel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

public class ConnectorMvccSnapshotTest {

    @Test
    public void anonymousImplExposesAccessors() {
        Instant now = Instant.parse("2024-06-01T00:00:00Z");
        ConnectorMvccSnapshot s = new ConnectorMvccSnapshot() {
            @Override
            public Instant commitTime() {
                return now;
            }

            @Override
            public Optional<ConnectorTableVersion> asVersion() {
                return Optional.of(new ConnectorTableVersion.BySnapshotId(42L));
            }

            @Override
            public String toOpaqueToken() {
                return "tok-42";
            }
        };
        Assertions.assertEquals(now, s.commitTime());
        Assertions.assertEquals("tok-42", s.toOpaqueToken());
        Assertions.assertTrue(s.asVersion().isPresent());
        Assertions.assertTrue(s.asVersion().get() instanceof ConnectorTableVersion.BySnapshotId);
    }

    @Test
    public void codecRoundTrip() {
        ConnectorMvccSnapshot original = new ConnectorMvccSnapshot() {
            @Override
            public Instant commitTime() {
                return Instant.parse("2024-06-01T00:00:00Z");
            }

            @Override
            public Optional<ConnectorTableVersion> asVersion() {
                return Optional.empty();
            }

            @Override
            public String toOpaqueToken() {
                return "abc";
            }
        };
        ConnectorMvccSnapshot.Codec codec = new ConnectorMvccSnapshot.Codec() {
            @Override
            public byte[] encode(ConnectorMvccSnapshot s) {
                return s.toOpaqueToken().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public ConnectorMvccSnapshot decode(byte[] bytes) {
                String token = new String(bytes, StandardCharsets.UTF_8);
                return new ConnectorMvccSnapshot() {
                    @Override
                    public Instant commitTime() {
                        return Instant.EPOCH;
                    }

                    @Override
                    public Optional<ConnectorTableVersion> asVersion() {
                        return Optional.empty();
                    }

                    @Override
                    public String toOpaqueToken() {
                        return token;
                    }
                };
            }
        };
        byte[] enc = codec.encode(original);
        ConnectorMvccSnapshot decoded = codec.decode(enc);
        Assertions.assertEquals(original.toOpaqueToken(), decoded.toOpaqueToken());
    }
}
