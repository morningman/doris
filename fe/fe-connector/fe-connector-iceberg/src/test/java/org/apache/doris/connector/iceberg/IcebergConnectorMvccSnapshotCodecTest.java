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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;

class IcebergConnectorMvccSnapshotCodecTest {

    @Test
    void roundTrip() {
        IcebergConnectorMvccSnapshot orig =
                new IcebergConnectorMvccSnapshot(123456789L, Instant.ofEpochMilli(1700000000000L));
        IcebergConnectorMvccSnapshot.Codec codec = new IcebergConnectorMvccSnapshot.Codec();
        byte[] bytes = codec.encode(orig);
        Assertions.assertEquals(IcebergConnectorMvccSnapshot.FRAME_BYTES, bytes.length);
        ConnectorMvccSnapshot back = codec.decode(bytes);
        Assertions.assertEquals(orig, back);
        Assertions.assertEquals(123456789L, ((IcebergConnectorMvccSnapshot) back).snapshotId());
        Assertions.assertEquals(1700000000000L, back.commitTime().toEpochMilli());
    }

    @Test
    void asVersionShape() {
        IcebergConnectorMvccSnapshot s =
                new IcebergConnectorMvccSnapshot(42L, Instant.ofEpochMilli(1L));
        Optional<ConnectorTableVersion> v = s.asVersion();
        Assertions.assertTrue(v.isPresent());
        Assertions.assertTrue(v.get() instanceof ConnectorTableVersion.BySnapshotId);
        Assertions.assertEquals(42L, ((ConnectorTableVersion.BySnapshotId) v.get()).snapshotId());
    }

    @Test
    void opaqueTokenFormat() {
        IcebergConnectorMvccSnapshot s =
                new IcebergConnectorMvccSnapshot(42L, Instant.ofEpochMilli(1700000000000L));
        Assertions.assertEquals("iceberg:42:1700000000000", s.toOpaqueToken());
    }

    @Test
    void decodeBadMagicRaises() {
        byte[] bad = new byte[IcebergConnectorMvccSnapshot.FRAME_BYTES];
        Arrays.fill(bad, (byte) 0xFF);
        IcebergConnectorMvccSnapshot.Codec codec = new IcebergConnectorMvccSnapshot.Codec();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> codec.decode(bad));
        Assertions.assertTrue(ex.getMessage().contains("bad magic"));
    }

    @Test
    void decodeWrongLengthRaises() {
        IcebergConnectorMvccSnapshot.Codec codec = new IcebergConnectorMvccSnapshot.Codec();
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> codec.decode(new byte[7]));
        Assertions.assertTrue(ex.getMessage().contains("frame length"));
    }
}
