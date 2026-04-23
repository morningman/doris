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

package org.apache.doris.connector.api.dispatch;

import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

class ConnectorDispatchOpsTest {

    private static final ConnectorTableHandle DUMMY_HANDLE = new ConnectorTableHandle() {
        private static final long serialVersionUID = 1L;
    };

    @Test
    void anonymousImplDelegatesOnIcebergInputFormat() {
        ConnectorDispatchOps ops = (handle, raw) -> {
            if ("iceberg".equalsIgnoreCase(raw.inputFormat())) {
                return Optional.of(DispatchTarget.builder("iceberg").build());
            }
            return Optional.empty();
        };

        RawTableMetadata icebergRaw = RawTableMetadata.of(
                Collections.emptyMap(), "iceberg", "s3://x", Collections.emptyMap());
        Optional<DispatchTarget> t = ops.resolveTarget(DUMMY_HANDLE, icebergRaw);
        Assertions.assertTrue(t.isPresent());
        Assertions.assertEquals("iceberg", t.get().connectorType());

        RawTableMetadata orcRaw = RawTableMetadata.of(
                Collections.emptyMap(), "OrcInputFormat", "s3://x", Collections.emptyMap());
        Assertions.assertTrue(ops.resolveTarget(DUMMY_HANDLE, orcRaw).isEmpty());

        RawTableMetadata emptyFmt = RawTableMetadata.of(
                Collections.emptyMap(), "", "", Collections.emptyMap());
        Assertions.assertTrue(ops.resolveTarget(DUMMY_HANDLE, emptyFmt).isEmpty());
    }
}
