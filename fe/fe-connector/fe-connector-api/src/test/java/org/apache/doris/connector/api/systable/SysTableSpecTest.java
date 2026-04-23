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

package org.apache.doris.connector.api.systable;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.cache.RefreshPolicy;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

public class SysTableSpecTest {

    private static ConnectorTableSchema schema() {
        ConnectorColumn c = new ConnectorColumn("snapshot_id", ConnectorType.of("bigint"), null,
                true, null);
        return new ConnectorTableSchema("snapshots", Collections.singletonList(c),
                "iceberg", Collections.emptyMap());
    }

    private static NativeSysTableScanFactory dummyFactory() {
        return (db, tbl, sys, version) -> (ConnectorScanPlanProvider) null;
    }

    private static TvfSysTableInvoker dummyInvoker() {
        return (db, tbl, sys, version) -> new TvfInvocation("partitions", Collections.emptyMap());
    }

    @Test
    public void nativeHappyPathDefaultsAreRefreshAlwaysAndNoVersion() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("snapshots")
                .schema(schema())
                .mode(SysTableExecutionMode.NATIVE)
                .nativeFactory(dummyFactory())
                .build();

        Assertions.assertEquals("snapshots", spec.name());
        Assertions.assertEquals(SysTableExecutionMode.NATIVE, spec.mode());
        Assertions.assertEquals(RefreshPolicy.ALWAYS_REFRESH, spec.refreshPolicy());
        Assertions.assertFalse(spec.dataTable());
        Assertions.assertFalse(spec.acceptsTableVersion());
        Assertions.assertTrue(spec.nativeFactory().isPresent());
        Assertions.assertTrue(spec.tvfInvoker().isEmpty());
        Assertions.assertTrue(spec.scanPlanProviderRef().isEmpty());
    }

    @Test
    public void tvfHappyPathWithFlagsAndVersion() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("partitions")
                .schema(schema())
                .mode(SysTableExecutionMode.TVF)
                .tvfInvoker(dummyInvoker())
                .dataTable(true)
                .acceptsTableVersion(true)
                .refreshPolicy(RefreshPolicy.TTL)
                .build();

        Assertions.assertTrue(spec.dataTable());
        Assertions.assertTrue(spec.acceptsTableVersion());
        Assertions.assertEquals(RefreshPolicy.TTL, spec.refreshPolicy());
        Assertions.assertTrue(spec.tvfInvoker().isPresent());
    }

    @Test
    public void connectorScanPlanRequiresRef() {
        SysTableSpec spec = SysTableSpec.builder()
                .name("custom")
                .schema(schema())
                .mode(SysTableExecutionMode.CONNECTOR_SCAN_PLAN)
                .scanPlanProviderRef(ConnectorScanPlanProviderRef.of("k1"))
                .build();
        Assertions.assertEquals(Optional.of(ConnectorScanPlanProviderRef.of("k1")),
                spec.scanPlanProviderRef());
    }

    @Test
    public void missingCarrierForModeRejected() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> SysTableSpec.builder()
                .name("snapshots").schema(schema()).mode(SysTableExecutionMode.NATIVE).build());
        Assertions.assertThrows(IllegalArgumentException.class, () -> SysTableSpec.builder()
                .name("partitions").schema(schema()).mode(SysTableExecutionMode.TVF).build());
        Assertions.assertThrows(IllegalArgumentException.class, () -> SysTableSpec.builder()
                .name("c").schema(schema()).mode(SysTableExecutionMode.CONNECTOR_SCAN_PLAN).build());
    }

    @Test
    public void rejectsNullsAndEmptyName() {
        Assertions.assertThrows(NullPointerException.class,
                () -> SysTableSpec.builder().name(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> SysTableSpec.builder().schema(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> SysTableSpec.builder().mode(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> SysTableSpec.builder().refreshPolicy(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> SysTableSpec.builder().nativeFactory(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> SysTableSpec.builder().tvfInvoker(null));
        Assertions.assertThrows(NullPointerException.class,
                () -> SysTableSpec.builder().scanPlanProviderRef(null));

        Assertions.assertThrows(IllegalArgumentException.class, () -> SysTableSpec.builder()
                .name("").schema(schema()).mode(SysTableExecutionMode.TVF)
                .tvfInvoker(dummyInvoker()).build());
    }

    @Test
    public void equalsHashCodeAndToString() {
        TvfSysTableInvoker inv = dummyInvoker();
        SysTableSpec a = SysTableSpec.builder().name("p").schema(schema())
                .mode(SysTableExecutionMode.TVF).tvfInvoker(inv).build();
        SysTableSpec b = SysTableSpec.builder().name("p").schema(schema())
                .mode(SysTableExecutionMode.TVF).tvfInvoker(inv).build();
        Assertions.assertEquals(a, b);
        Assertions.assertEquals(a.hashCode(), b.hashCode());
        Assertions.assertTrue(a.toString().contains("name=p"));
        Assertions.assertTrue(a.toString().contains("TVF"));
    }
}
