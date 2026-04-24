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

package org.apache.doris.connector.iceberg.backend.dlf;

import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;
import org.apache.doris.connector.iceberg.api.IcebergBackendContext;
import org.apache.doris.connector.iceberg.api.IcebergBackendException;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * The DLF backend is still a stub (M1-04 anomaly #4): its
 * {@code buildCatalog} throws {@link IcebergBackendException}. That same
 * exception therefore propagates through the default {@code listRefs} /
 * {@code resolveVersion} implementations inherited from
 * {@code IcebergBackend}. This test locks that behaviour in so downstream
 * consumers can rely on a clear failure instead of a classloader error.
 */
class DlfIcebergBackendTimeTravelTest {

    private static IcebergBackendContext ctx() {
        return new IcebergBackendContext("c", new HashMap<>(), new Configuration(false));
    }

    @Test
    void listRefsPropagatesStubException() {
        DlfIcebergBackend backend = new DlfIcebergBackend();
        Assertions.assertThrows(IcebergBackendException.class,
                () -> backend.listRefs(ctx(), "db", "t"));
    }

    @Test
    void resolveVersionPropagatesStubException() {
        DlfIcebergBackend backend = new DlfIcebergBackend();
        Assertions.assertThrows(IcebergBackendException.class,
                () -> backend.resolveVersion(ctx(), "db", "t",
                        new ConnectorTableVersion.BySnapshotId(1L)));
    }
}
