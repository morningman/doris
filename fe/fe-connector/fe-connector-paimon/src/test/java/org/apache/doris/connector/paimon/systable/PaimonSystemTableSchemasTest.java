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

package org.apache.doris.connector.paimon.systable;

import org.apache.doris.connector.api.ConnectorColumn;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class PaimonSystemTableSchemasTest {

    @Test
    void supportedHasThirteenSysTables() {
        Map<String, List<ConnectorColumn>> map = PaimonSystemTableSchemas.supported();
        Assertions.assertEquals(13, map.size());
        Assertions.assertTrue(map.containsKey("snapshots"));
        Assertions.assertTrue(map.containsKey("schemas"));
        Assertions.assertTrue(map.containsKey("options"));
        Assertions.assertTrue(map.containsKey("tags"));
        Assertions.assertTrue(map.containsKey("branches"));
        Assertions.assertTrue(map.containsKey("consumers"));
        Assertions.assertTrue(map.containsKey("aggregation_fields"));
        Assertions.assertTrue(map.containsKey("files"));
        Assertions.assertTrue(map.containsKey("manifests"));
        Assertions.assertTrue(map.containsKey("partitions"));
        Assertions.assertTrue(map.containsKey("statistics"));
        Assertions.assertTrue(map.containsKey("buckets"));
        Assertions.assertTrue(map.containsKey("table_indexes"));
    }

    @Test
    void supportedMapIsImmutable() {
        Map<String, List<ConnectorColumn>> map = PaimonSystemTableSchemas.supported();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> map.put("foo", List.of()));
    }

    @Test
    void columnsOfReturnsSameListAcrossCalls() {
        // Schemas must be deterministic — same instance across lookups.
        Assertions.assertSame(PaimonSystemTableSchemas.columnsOf("snapshots"),
                PaimonSystemTableSchemas.columnsOf("snapshots"));
    }

    @Test
    void columnsOfIsCaseInsensitive() {
        Assertions.assertSame(PaimonSystemTableSchemas.columnsOf("snapshots"),
                PaimonSystemTableSchemas.columnsOf("SNAPSHOTS"));
        Assertions.assertNotNull(PaimonSystemTableSchemas.columnsOf("Aggregation_Fields"));
    }

    @Test
    void columnsOfRejectsUnknown() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonSystemTableSchemas.columnsOf("audit_log"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> PaimonSystemTableSchemas.columnsOf("not-a-thing"));
    }

    @Test
    void columnsOfRejectsNull() {
        Assertions.assertThrows(NullPointerException.class,
                () -> PaimonSystemTableSchemas.columnsOf(null));
    }

    @Test
    void everySchemaIsNonEmptyAndAllColumnsNullable() {
        for (Map.Entry<String, List<ConnectorColumn>> e
                : PaimonSystemTableSchemas.supported().entrySet()) {
            String name = e.getKey();
            List<ConnectorColumn> cols = e.getValue();
            Assertions.assertFalse(cols.isEmpty(), name + " columns empty");
            for (ConnectorColumn col : cols) {
                Assertions.assertTrue(col.isNullable(),
                        name + "." + col.getName() + " should be exposed as nullable");
                Assertions.assertNotNull(col.getType(),
                        name + "." + col.getName() + " missing type");
                Assertions.assertEquals(col.getName().toLowerCase(java.util.Locale.ROOT),
                        col.getName(),
                        name + "." + col.getName() + " name should be lower-case");
            }
        }
    }

    @Test
    void columnListsAreImmutable() {
        for (List<ConnectorColumn> cols : PaimonSystemTableSchemas.supported().values()) {
            Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> cols.add(null));
        }
    }
}
