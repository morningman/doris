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

package org.apache.doris.connector.timetravel;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.RefKind;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ConnectorRefSpecResolverTest {

    @Test
    public void nullParamsReturnsEmpty() {
        Assertions.assertTrue(ConnectorRefSpecResolver.resolve(null).isEmpty());
    }

    @Test
    public void incrementalReadReturnsEmpty() {
        TableScanParams params = new TableScanParams(
                TableScanParams.INCREMENTAL_READ, ImmutableMap.of(), ImmutableList.of());
        Assertions.assertTrue(ConnectorRefSpecResolver.resolve(params).isEmpty());
    }

    @Test
    public void resolvesBranchFromMapForm() {
        TableScanParams params = new TableScanParams(
                TableScanParams.BRANCH,
                ImmutableMap.of(TableScanParams.PARAMS_NAME, "main"),
                ImmutableList.of());
        Optional<ConnectorRefSpec> spec = ConnectorRefSpecResolver.resolve(params);
        Assertions.assertTrue(spec.isPresent());
        Assertions.assertEquals("main", spec.get().name());
        Assertions.assertEquals(RefKind.BRANCH, spec.get().kind());
        Assertions.assertTrue(spec.get().snapshotId().isEmpty());
        Assertions.assertTrue(spec.get().retentionMs().isEmpty());
    }

    @Test
    public void resolvesBranchFromListForm() {
        TableScanParams params = new TableScanParams(
                TableScanParams.BRANCH,
                ImmutableMap.of(),
                ImmutableList.of("dev"));
        Optional<ConnectorRefSpec> spec = ConnectorRefSpecResolver.resolve(params);
        Assertions.assertTrue(spec.isPresent());
        Assertions.assertEquals("dev", spec.get().name());
        Assertions.assertEquals(RefKind.BRANCH, spec.get().kind());
    }

    @Test
    public void resolvesTagFromMapForm() {
        TableScanParams params = new TableScanParams(
                TableScanParams.TAG,
                ImmutableMap.of(TableScanParams.PARAMS_NAME, "v1.0"),
                ImmutableList.of());
        Optional<ConnectorRefSpec> spec = ConnectorRefSpecResolver.resolve(params);
        Assertions.assertTrue(spec.isPresent());
        Assertions.assertEquals("v1.0", spec.get().name());
        Assertions.assertEquals(RefKind.TAG, spec.get().kind());
    }

    @Test
    public void rejectsMissingName() {
        TableScanParams params = new TableScanParams(
                TableScanParams.BRANCH, ImmutableMap.of(), ImmutableList.of());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorRefSpecResolver.resolve(params));
    }

    @Test
    public void mapFormTakesPriorityOverList() {
        TableScanParams params = new TableScanParams(
                TableScanParams.TAG,
                ImmutableMap.of(TableScanParams.PARAMS_NAME, "from-map"),
                ImmutableList.of("from-list"));
        Optional<ConnectorRefSpec> spec = ConnectorRefSpecResolver.resolve(params);
        Assertions.assertTrue(spec.isPresent());
        Assertions.assertEquals("from-map", spec.get().name());
    }
}
