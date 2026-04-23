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

package org.apache.doris.connector.api.write;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class ConnectorTransactionContextTest {

    @Test
    public void anonymousImpl() {
        ConnectorTransactionContext ctx = new ConnectorTransactionContext() {
            @Override
            public String txnId() {
                return "txn-42";
            }

            @Override
            public String label() {
                return "label-42";
            }

            @Override
            public byte[] serialize() {
                return "ctx-bytes".getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public boolean supportsFailover() {
                return true;
            }
        };
        Assertions.assertEquals("txn-42", ctx.txnId());
        Assertions.assertEquals("label-42", ctx.label());
        Assertions.assertArrayEquals("ctx-bytes".getBytes(StandardCharsets.UTF_8), ctx.serialize());
        Assertions.assertTrue(ctx.supportsFailover());
    }
}
