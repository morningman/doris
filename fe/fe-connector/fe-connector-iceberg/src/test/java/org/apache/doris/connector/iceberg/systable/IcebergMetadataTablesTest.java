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

package org.apache.doris.connector.iceberg.systable;

import org.apache.doris.connector.api.ConnectorColumn;

import org.apache.iceberg.MetadataTableType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class IcebergMetadataTablesTest {

    @Test
    void supportedMapHasExactlySevenEntriesInPublicationOrder() {
        Map<String, MetadataTableType> supported = IcebergMetadataTables.supported();
        Assertions.assertEquals(7, supported.size());
        Assertions.assertEquals(MetadataTableType.SNAPSHOTS, supported.get("snapshots"));
        Assertions.assertEquals(MetadataTableType.HISTORY, supported.get("history"));
        Assertions.assertEquals(MetadataTableType.FILES, supported.get("files"));
        Assertions.assertEquals(MetadataTableType.ENTRIES, supported.get("entries"));
        Assertions.assertEquals(MetadataTableType.MANIFESTS, supported.get("manifests"));
        Assertions.assertEquals(MetadataTableType.REFS, supported.get("refs"));
        Assertions.assertEquals(MetadataTableType.PARTITIONS, supported.get("partitions"));
    }

    @Test
    void columnsOfReturnsImmutableOrderedColumnList() {
        List<ConnectorColumn> cols = IcebergMetadataTables.columnsOf(MetadataTableType.SNAPSHOTS);
        Assertions.assertEquals("committed_at", cols.get(0).getName());
        Assertions.assertEquals("snapshot_id", cols.get(1).getName());
        Assertions.assertEquals("summary", cols.get(cols.size() - 1).getName());
    }

    @Test
    void columnsOfRejectsUnsupportedType() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergMetadataTables.columnsOf(MetadataTableType.POSITION_DELETES));
    }

    @Test
    void columnsOfRejectsNull() {
        Assertions.assertThrows(NullPointerException.class,
                () -> IcebergMetadataTables.columnsOf(null));
    }

    @Test
    void allColumnsAreNullable() {
        for (MetadataTableType t : IcebergMetadataTables.supported().values()) {
            for (ConnectorColumn c : IcebergMetadataTables.columnsOf(t)) {
                Assertions.assertTrue(c.isNullable(),
                        t + "." + c.getName() + " must be nullable (iceberg metadata semantics)");
            }
        }
    }

    @Test
    void supportedMapIsImmutable() {
        Map<String, MetadataTableType> supported = IcebergMetadataTables.supported();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> supported.put("xxx", MetadataTableType.POSITION_DELETES));
    }
}
