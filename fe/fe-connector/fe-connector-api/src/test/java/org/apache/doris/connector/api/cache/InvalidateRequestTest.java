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

package org.apache.doris.connector.api.cache;

import org.apache.doris.connector.api.ConnectorTableId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class InvalidateRequestTest {

    @Test
    public void ofCatalogPopulatesScope() {
        InvalidateRequest r = InvalidateRequest.ofCatalog();
        Assertions.assertEquals(InvalidateScope.CATALOG, r.getScope());
        Assertions.assertFalse(r.getDatabase().isPresent());
        Assertions.assertFalse(r.getTable().isPresent());
        Assertions.assertTrue(r.getPartitionKeys().isEmpty());
        Assertions.assertFalse(r.getSysName().isPresent());
    }

    @Test
    public void ofDatabaseRequiresDatabase() {
        InvalidateRequest r = InvalidateRequest.ofDatabase("db");
        Assertions.assertEquals(InvalidateScope.DATABASE, r.getScope());
        Assertions.assertEquals("db", r.getDatabase().orElse(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> InvalidateRequest.ofDatabase(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> InvalidateRequest.ofDatabase(""));
    }

    @Test
    public void ofTableRequiresBoth() {
        InvalidateRequest r = InvalidateRequest.ofTable(ConnectorTableId.of("db", "t"));
        Assertions.assertEquals(InvalidateScope.TABLE, r.getScope());
        Assertions.assertEquals("db", r.getDatabase().orElse(null));
        Assertions.assertEquals("t", r.getTable().orElse(null));
        Assertions.assertTrue(r.getPartitionKeys().isEmpty());
        Assertions.assertThrows(NullPointerException.class, () -> InvalidateRequest.ofTable(null));
        Assertions.assertThrows(NullPointerException.class, () -> ConnectorTableId.of("db", null));

    }

    @Test
    public void ofPartitionsCopiesAndIsImmutable() {
        List<String> input = Arrays.asList("p1", "p2");
        InvalidateRequest r = InvalidateRequest.ofPartitions(ConnectorTableId.of("db", "t"), input);
        Assertions.assertEquals(InvalidateScope.PARTITIONS, r.getScope());
        Assertions.assertEquals(Arrays.asList("p1", "p2"), r.getPartitionKeys());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> r.getPartitionKeys().add("p3"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> InvalidateRequest.ofPartitions(ConnectorTableId.of("db", "t"), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> InvalidateRequest.ofPartitions(ConnectorTableId.of("db", "t"), Collections.singletonList(null)));
    }

    @Test
    public void ofSysTableRequiresSysName() {
        InvalidateRequest r = InvalidateRequest.ofSysTable(ConnectorTableId.of("db", "t"), "snapshots");
        Assertions.assertEquals(InvalidateScope.SYS_TABLE, r.getScope());
        Assertions.assertEquals("snapshots", r.getSysName().orElse(null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> InvalidateRequest.ofSysTable(ConnectorTableId.of("db", "t"), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> InvalidateRequest.ofSysTable(ConnectorTableId.of("db", "t"), ""));
    }

    @Test
    public void equalsAndHashCode() {
        Assertions.assertEquals(InvalidateRequest.ofTable(ConnectorTableId.of("db", "t")),
                InvalidateRequest.ofTable(ConnectorTableId.of("db", "t")));
        Assertions.assertEquals(InvalidateRequest.ofTable(ConnectorTableId.of("db", "t")).hashCode(),
                InvalidateRequest.ofTable(ConnectorTableId.of("db", "t")).hashCode());
        Assertions.assertNotEquals(InvalidateRequest.ofTable(ConnectorTableId.of("db", "t")),
                InvalidateRequest.ofTable(ConnectorTableId.of("db", "u")));
        Assertions.assertTrue(InvalidateRequest.ofCatalog().toString().contains("CATALOG"));
    }
}
