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
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorTransactionContext;
import org.apache.doris.connector.api.write.ConnectorTxnCapability;
import org.apache.doris.connector.api.write.WriteIntent;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

class IcebergTransactionContextTest {

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

    private Table createTable(String name) {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "region", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("region").build();
        return catalog.createTable(TableIdentifier.of("db", name), schema, spec);
    }

    private ConnectorTableHandle handleFor(String name) {
        return metadata.getTableHandle(null, "db", name).orElseThrow();
    }

    @Test
    void connectorAdvertisesIcebergCapabilities() {
        Assertions.assertEquals(IcebergTransactionContext.CAPABILITIES, metadata.txnCapabilities());
        Assertions.assertTrue(metadata.txnCapabilities().contains(ConnectorTxnCapability.MULTI_STATEMENT));
        Assertions.assertTrue(metadata.txnCapabilities().contains(ConnectorTxnCapability.COMMIT_RETRY));
        Assertions.assertTrue(metadata.txnCapabilities().contains(ConnectorTxnCapability.FAILOVER_SAFE));
        Assertions.assertFalse(metadata.txnCapabilities().contains(ConnectorTxnCapability.SAVEPOINT));
    }

    @Test
    void beginTransactionReturnsIcebergContext() {
        createTable("t");
        ConnectorTransactionContext ctx = metadata.beginTransaction(
                null, handleFor("t"), WriteIntent.simple());
        Assertions.assertTrue(ctx instanceof IcebergTransactionContext);
        IcebergTransactionContext ice = (IcebergTransactionContext) ctx;
        Assertions.assertEquals("db", ice.getDbName());
        Assertions.assertEquals("t", ice.getTableName());
        Assertions.assertNotNull(ice.getTransaction());
        Assertions.assertNotNull(ice.getTable());
    }

    @Test
    void txnIdStableAcrossCalls() {
        createTable("t");
        IcebergTransactionContext ctx = (IcebergTransactionContext) metadata.beginTransaction(
                null, handleFor("t"), WriteIntent.simple());
        String first = ctx.txnId();
        Assertions.assertEquals(first, ctx.txnId());
        Assertions.assertTrue(first.startsWith("iceberg-"));
    }

    @Test
    void labelEncodesOverwriteModeAndBranch() {
        createTable("t");
        WriteIntent intent = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE)
                .build();
        IcebergTransactionContext ctx = (IcebergTransactionContext) metadata.beginTransaction(
                null, handleFor("t"), intent);
        Assertions.assertTrue(ctx.label().contains("db.t"));
        Assertions.assertTrue(ctx.label().contains("FULL_TABLE"));
    }

    @Test
    void contextCapabilitiesEqualConnectorCapabilities() {
        createTable("t");
        ConnectorTransactionContext ctx = metadata.beginTransaction(
                null, handleFor("t"), WriteIntent.simple());
        Assertions.assertEquals(metadata.txnCapabilities(), ctx.txnCapabilities());
    }

    @Test
    void supportsFailoverIsFalseUntilSerializerLands() {
        createTable("t");
        ConnectorTransactionContext ctx = metadata.beginTransaction(
                null, handleFor("t"), WriteIntent.simple());
        Assertions.assertFalse(ctx.supportsFailover());
        Assertions.assertThrows(DorisConnectorException.class, ctx::serialize);
    }
}
