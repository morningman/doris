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

package org.apache.doris.connector.paimon.backend.aliyundlf;

import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.paimon.api.PaimonBackendContext;
import org.apache.doris.connector.paimon.api.PaimonBackendException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * Verifies that the aliyun-dlf stub surfaces {@link PaimonBackendException}
 * through the inherited default {@code listRefs} / {@code resolveVersion}
 * methods (because {@code buildCatalog} is itself a stub that throws).
 * Real implementation arrives in M3 alongside the iceberg DLF backend.
 */
class AliyunDlfPaimonBackendRefOpsTest {

    private PaimonBackendContext ctx() {
        return new PaimonBackendContext("c", new HashMap<>());
    }

    @Test
    void listRefsSurfacesStubException() {
        AliyunDlfPaimonBackend backend = new AliyunDlfPaimonBackend();
        Assertions.assertThrows(PaimonBackendException.class,
                () -> backend.listRefs(ctx(), "db", "t"));
    }

    @Test
    void resolveVersionSurfacesStubException() {
        AliyunDlfPaimonBackend backend = new AliyunDlfPaimonBackend();
        Assertions.assertThrows(PaimonBackendException.class,
                () -> backend.resolveVersion(ctx(), "db", "t",
                        new ConnectorTableVersion.BySnapshotId(1L)));
    }
}
