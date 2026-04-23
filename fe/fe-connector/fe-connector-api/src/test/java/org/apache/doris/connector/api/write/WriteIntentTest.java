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

package org.apache.doris.connector.api.write;

import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class WriteIntentTest {

    @Test
    public void simpleDefaults() {
        WriteIntent s = WriteIntent.simple();
        Assertions.assertSame(s, WriteIntent.simple());
        Assertions.assertEquals(WriteIntent.OverwriteMode.NONE, s.overwriteMode());
        Assertions.assertFalse(s.isUpsert());
        Assertions.assertEquals(Optional.empty(), s.branch());
        Assertions.assertTrue(s.staticPartitions().isEmpty());
        Assertions.assertEquals(DeleteMode.COW, s.deleteMode());
        Assertions.assertEquals(Optional.empty(), s.writeAtVersion());
    }

    @Test
    public void staticPartitionRequiresMap() {
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> WriteIntent.builder().overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION).build());
        Assertions.assertTrue(ex.getMessage().contains("STATIC_PARTITION"));
    }

    @Test
    public void nonStaticModeRejectsStaticPartitions() {
        Map<String, String> parts = new HashMap<>();
        parts.put("dt", "2024-01-01");
        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> WriteIntent.builder()
                        .overwriteMode(WriteIntent.OverwriteMode.DYNAMIC_PARTITION)
                        .staticPartitions(parts)
                        .build());
        Assertions.assertTrue(ex.getMessage().contains("staticPartitions"));
    }

    @Test
    public void staticPartitionsImmutable() {
        Map<String, String> parts = new LinkedHashMap<>();
        parts.put("dt", "2024-01-01");
        parts.put("region", "us");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.STATIC_PARTITION)
                .staticPartitions(parts)
                .build();
        parts.put("leak", "x");
        Assertions.assertEquals(2, intent.staticPartitions().size());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> intent.staticPartitions().put("k", "v"));
    }

    @Test
    public void branchAndVersionRoundTrip() {
        ConnectorTableVersion v = new ConnectorTableVersion.BySnapshotId(42L);
        WriteIntent intent = WriteIntent.builder()
                .branch("main")
                .upsert(true)
                .deleteMode(DeleteMode.EQUALITY_DELETE)
                .writeAtVersion(v)
                .build();
        Assertions.assertEquals(Optional.of("main"), intent.branch());
        Assertions.assertTrue(intent.isUpsert());
        Assertions.assertEquals(DeleteMode.EQUALITY_DELETE, intent.deleteMode());
        Assertions.assertEquals(Optional.of(v), intent.writeAtVersion());
    }

    @Test
    public void equalsHashCodeToString() {
        WriteIntent a = WriteIntent.builder().branch("b").upsert(true).build();
        WriteIntent b = WriteIntent.builder().branch("b").upsert(true).build();
        WriteIntent c = WriteIntent.builder().branch("b").upsert(false).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertNotEquals(a, c);
        Assertions.assertTrue(a.toString().contains("WriteIntent"));
    }
}
