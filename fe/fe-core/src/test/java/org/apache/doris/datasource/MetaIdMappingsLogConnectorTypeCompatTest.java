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

package org.apache.doris.datasource;

import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetaIdMappingsLogConnectorTypeCompatTest {

    @Test
    public void newInstanceDefaultsToHive() {
        MetaIdMappingsLog log = new MetaIdMappingsLog();
        Assertions.assertEquals("hive", log.getConnectorType());
    }

    @Test
    public void deserializingOldImageWithoutFieldDefaultsToHive() {
        // Pre-M2-01 image: no "connectorType" key.
        String json = "{\"ctlId\":42,\"fromEvent\":true,\"lastEventId\":100,\"metaIdMappings\":[]}";
        MetaIdMappingsLog log = GsonUtils.GSON.fromJson(json, MetaIdMappingsLog.class);
        Assertions.assertEquals(42L, log.getCatalogId());
        Assertions.assertTrue(log.isFromHmsEvent());
        Assertions.assertEquals(100L, log.getLastSyncedEventId());
        Assertions.assertEquals("hive", log.getConnectorType(),
                "missing connectorType field must default to \"hive\"");
    }

    @Test
    public void deserializingExplicitNullStillFallsBackToHiveAfterRead() throws Exception {
        // Simulate a hand-crafted JSON where connectorType is explicitly null.
        String json = "{\"ctlId\":7,\"connectorType\":null,\"metaIdMappings\":[]}";
        MetaIdMappingsLog viaRead = readViaWritable(json);
        Assertions.assertEquals("hive", viaRead.getConnectorType());
    }

    @Test
    public void roundTripsCustomConnectorType() {
        MetaIdMappingsLog log = new MetaIdMappingsLog();
        log.setCatalogId(1L);
        log.setLastSyncedEventId(99L);
        log.setConnectorType("iceberg");
        String json = GsonUtils.GSON.toJson(log);
        MetaIdMappingsLog back = GsonUtils.GSON.fromJson(json, MetaIdMappingsLog.class);
        Assertions.assertEquals("iceberg", back.getConnectorType());
        Assertions.assertEquals(log, back);
    }

    @Test
    public void roundTripsHiveAsWritten() {
        MetaIdMappingsLog log = new MetaIdMappingsLog();
        log.setCatalogId(2L);
        log.setLastSyncedEventId(123L);
        // do not set connectorType -> defaults to "hive"
        String json = GsonUtils.GSON.toJson(log);
        Assertions.assertTrue(json.contains("\"connectorType\":\"hive\""), json);
        MetaIdMappingsLog back = GsonUtils.GSON.fromJson(json, MetaIdMappingsLog.class);
        Assertions.assertEquals("hive", back.getConnectorType());
    }

    @Test
    public void hashCodeAndEqualsHonorConnectorType() {
        MetaIdMappingsLog a = new MetaIdMappingsLog();
        a.setCatalogId(5L);
        a.setConnectorType("iceberg");
        MetaIdMappingsLog b = new MetaIdMappingsLog();
        b.setCatalogId(5L);
        b.setConnectorType("paimon");
        Assertions.assertNotEquals(a, b);
        Assertions.assertNotEquals(a.hashCode(), b.hashCode());
        b.setConnectorType("iceberg");
        Assertions.assertEquals(a, b);
    }

    private static MetaIdMappingsLog readViaWritable(String json) throws Exception {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.DataOutputStream dos = new java.io.DataOutputStream(baos)) {
            org.apache.doris.common.io.Text.writeString(dos, json);
        }
        try (java.io.DataInputStream dis = new java.io.DataInputStream(
                new java.io.ByteArrayInputStream(baos.toByteArray()))) {
            return MetaIdMappingsLog.read(dis);
        }
    }
}
