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

import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class IcebergConnectorMetadataColumnHandlesTest {

    private InMemoryCatalog catalog;
    private IcebergConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        ((SupportsNamespaces) catalog).createNamespace(Namespace.of("db"));
        metadata = new IcebergConnectorMetadata(catalog, new HashMap<>(), null, null, null);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void primitiveSchemaProducesOneHandlePerColumnInOrder() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get(), "user name"),
                Types.NestedField.optional(3, "score", Types.DoubleType.get()));
        catalog.createTable(TableIdentifier.of("db", "t1"), schema);
        ConnectorTableHandle handle = metadata
                .getTableHandle(null, "db", "t1").orElseThrow();

        Map<String, ConnectorColumnHandle> result =
                metadata.getColumnHandles(null, handle);

        Assertions.assertEquals(3, result.size());
        Iterator<Map.Entry<String, ConnectorColumnHandle>> it = result.entrySet().iterator();
        Map.Entry<String, ConnectorColumnHandle> e0 = it.next();
        Assertions.assertEquals("id", e0.getKey());
        IcebergColumnHandle id = (IcebergColumnHandle) e0.getValue();
        Assertions.assertEquals(1, id.getFieldId());
        Assertions.assertEquals(Types.LongType.get(), id.getIcebergType());
        Assertions.assertFalse(id.isNullable());
        Assertions.assertEquals(0, id.getPosition());

        Map.Entry<String, ConnectorColumnHandle> e1 = it.next();
        Assertions.assertEquals("name", e1.getKey());
        IcebergColumnHandle name = (IcebergColumnHandle) e1.getValue();
        Assertions.assertTrue(name.isNullable());
        Assertions.assertEquals("user name", name.getComment());
        Assertions.assertEquals(1, name.getPosition());

        Map.Entry<String, ConnectorColumnHandle> e2 = it.next();
        Assertions.assertEquals("score", e2.getKey());
        Assertions.assertEquals(2, ((IcebergColumnHandle) e2.getValue()).getPosition());
    }

    @Test
    void nestedStructListAndMapAreReturnedAsIcebergTypes() {
        Type structType = Types.StructType.of(
                Types.NestedField.required(11, "x", Types.IntegerType.get()),
                Types.NestedField.optional(12, "y", Types.StringType.get()));
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "addr", structType),
                Types.NestedField.optional(3, "tags",
                        Types.ListType.ofRequired(13, Types.StringType.get())),
                Types.NestedField.optional(4, "props",
                        Types.MapType.ofRequired(14, 15,
                                Types.StringType.get(), Types.LongType.get())));
        catalog.createTable(TableIdentifier.of("db", "t2"), schema);
        ConnectorTableHandle handle = metadata
                .getTableHandle(null, "db", "t2").orElseThrow();

        Map<String, ConnectorColumnHandle> result =
                metadata.getColumnHandles(null, handle);

        Assertions.assertEquals(4, result.size());
        IcebergColumnHandle addr = (IcebergColumnHandle) result.get("addr");
        Assertions.assertTrue(addr.getIcebergType().isStructType());
        IcebergColumnHandle tags = (IcebergColumnHandle) result.get("tags");
        Assertions.assertTrue(tags.getIcebergType().isListType());
        IcebergColumnHandle props = (IcebergColumnHandle) result.get("props");
        Assertions.assertTrue(props.getIcebergType().isMapType());
        // top-level only — nested fields are not flattened
        List<String> names = new ArrayList<>(result.keySet());
        Assertions.assertEquals(List.of("id", "addr", "tags", "props"), names);
    }

    @Test
    void schemaIdSelectsHistoricalSchemaWhenSet() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()));
        Table table = catalog.createTable(TableIdentifier.of("db", "t3"), schema);
        int originalSchemaId = table.schema().schemaId();

        // Evolve schema → new schema id.
        table.updateSchema().addColumn("extra", Types.IntegerType.get()).commit();
        table.refresh();
        Assertions.assertNotEquals(originalSchemaId, table.schema().schemaId());

        IcebergTableHandle current = (IcebergTableHandle) metadata
                .getTableHandle(null, "db", "t3").orElseThrow();
        Map<String, ConnectorColumnHandle> currentCols =
                metadata.getColumnHandles(null, current);
        Assertions.assertEquals(3, currentCols.size());
        Assertions.assertTrue(currentCols.containsKey("extra"));

        IcebergTableHandle pinned = current.toBuilder()
                .schemaId(originalSchemaId).build();
        Map<String, ConnectorColumnHandle> historicalCols =
                metadata.getColumnHandles(null, pinned);
        Assertions.assertEquals(2, historicalCols.size());
        Assertions.assertFalse(historicalCols.containsKey("extra"));
    }

    @Test
    void getTableHandlePopulatesIcebergMetadataFields() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "p", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        catalog.createTable(TableIdentifier.of("db", "t4"), schema, spec);

        IcebergTableHandle handle = (IcebergTableHandle) metadata
                .getTableHandle(null, "db", "t4").orElseThrow();

        Assertions.assertEquals("db", handle.getDbName());
        Assertions.assertEquals("t4", handle.getTableName());
        Assertions.assertNotNull(handle.getFormatVersion());
        Assertions.assertNotNull(handle.getSchemaId());
        Assertions.assertNotNull(handle.getPartitionSpecId());
        Assertions.assertNotNull(handle.getMetadataLocation());
        Assertions.assertNull(handle.getRefSpec(),
                "refSpec is wired in M2-Iceberg-07; must remain null here");
        // table has no snapshot yet (no data committed) → snapshotId may be null.
    }

    @Test
    void unknownSchemaIdThrowsIllegalState() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()));
        catalog.createTable(TableIdentifier.of("db", "t5"), schema);
        IcebergTableHandle current = (IcebergTableHandle) metadata
                .getTableHandle(null, "db", "t5").orElseThrow();
        IcebergTableHandle bogus = current.toBuilder().schemaId(9999).build();
        Assertions.assertThrows(IllegalStateException.class,
                () -> metadata.getColumnHandles(null, bogus));
    }
}
