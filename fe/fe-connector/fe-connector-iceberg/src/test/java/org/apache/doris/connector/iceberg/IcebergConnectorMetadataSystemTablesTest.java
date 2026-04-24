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

import org.apache.doris.connector.api.systable.SysTableExecutionMode;
import org.apache.doris.connector.api.systable.SysTableSpec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

class IcebergConnectorMetadataSystemTablesTest {

    @Test
    void exposesSevenSystemTablesViaConnectorMetadataSurface() {
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(
                null, new HashMap<>(), null, null, null);
        List<SysTableSpec> specs = metadata.listSysTables("db", "tbl");
        Assertions.assertEquals(7, specs.size());
        for (SysTableSpec spec : specs) {
            Assertions.assertEquals(SysTableExecutionMode.NATIVE, spec.mode());
            Assertions.assertFalse(spec.acceptsTableVersion());
            Assertions.assertTrue(spec.nativeFactory().isPresent());
        }
    }

    @Test
    void getSysTableRoutesByName() {
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(
                null, new HashMap<>(), null, null, null);
        Assertions.assertTrue(metadata.getSysTable("db", "tbl", "snapshots").isPresent());
        Assertions.assertTrue(metadata.getSysTable("db", "tbl", "history").isPresent());
        Assertions.assertTrue(metadata.getSysTable("db", "tbl", "refs").isPresent());
        Assertions.assertEquals(Optional.empty(),
                metadata.getSysTable("db", "tbl", "position_deletes"));
    }

    @Test
    void supportsSysTableDelegates() {
        IcebergConnectorMetadata metadata = new IcebergConnectorMetadata(
                null, new HashMap<>(), null, null, null);
        Assertions.assertTrue(metadata.supportsSysTable("db", "tbl", "files"));
        Assertions.assertFalse(metadata.supportsSysTable("db", "tbl", "nope"));
    }
}
