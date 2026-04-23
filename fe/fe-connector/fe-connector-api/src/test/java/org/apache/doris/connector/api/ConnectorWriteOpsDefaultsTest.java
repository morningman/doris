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

package org.apache.doris.connector.api;

import org.apache.doris.connector.api.handle.ConnectorInsertHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.WriteIntent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class ConnectorWriteOpsDefaultsTest {

    private static final ConnectorInsertHandle SENTINEL = new ConnectorInsertHandle() { };

    private static ConnectorWriteOps newOps() {
        return new ConnectorWriteOps() {
            @Override
            public boolean supportsInsert() {
                return true;
            }

            @Override
            public ConnectorInsertHandle beginInsert(
                    ConnectorSession session,
                    ConnectorTableHandle handle,
                    List<ConnectorColumn> columns) {
                return SENTINEL;
            }
        };
    }

    @Test
    public void overloadDelegatesToThreeArg() {
        ConnectorWriteOps ops = newOps();
        ConnectorInsertHandle got = ops.beginInsert(null, null, Collections.emptyList(), WriteIntent.simple());
        Assertions.assertSame(SENTINEL, got);
    }

    @Test
    public void txnCapabilitiesDefaultsEmpty() {
        ConnectorWriteOps ops = newOps();
        Assertions.assertTrue(ops.txnCapabilities().isEmpty());
    }

    @Test
    public void nullIntentThrowsNpe() {
        ConnectorWriteOps ops = newOps();
        Assertions.assertThrows(NullPointerException.class,
                () -> ops.beginInsert(null, null, Collections.emptyList(), null));
    }
}
