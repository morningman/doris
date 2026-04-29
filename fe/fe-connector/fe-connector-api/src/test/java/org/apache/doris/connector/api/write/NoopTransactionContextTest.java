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

public class NoopTransactionContextTest {

    @Test
    public void txnIdIsStableAndUnique() {
        NoopTransactionContext a = new NoopTransactionContext("label-a");
        NoopTransactionContext b = new NoopTransactionContext("label-b");
        Assertions.assertNotNull(a.txnId());
        Assertions.assertTrue(a.txnId().startsWith("noop-"));
        Assertions.assertEquals(a.txnId(), a.txnId());
        Assertions.assertNotEquals(a.txnId(), b.txnId());
    }

    @Test
    public void labelIsPreserved() {
        NoopTransactionContext ctx = new NoopTransactionContext("my-label");
        Assertions.assertEquals("my-label", ctx.label());
    }

    @Test
    public void capabilitiesAreEmpty() {
        NoopTransactionContext ctx = new NoopTransactionContext("x");
        Assertions.assertTrue(ctx.txnCapabilities().isEmpty());
        Assertions.assertFalse(ctx.supportsFailover());
    }

    @Test
    public void serializeIsUnsupported() {
        NoopTransactionContext ctx = new NoopTransactionContext("x");
        Assertions.assertThrows(UnsupportedOperationException.class, ctx::serialize);
    }

    @Test
    public void forIntentDerivesLabelFromOverwriteMode() {
        NoopTransactionContext simple = NoopTransactionContext.forIntent(WriteIntent.simple());
        Assertions.assertTrue(simple.label().contains(WriteIntent.OverwriteMode.NONE.name()));
        WriteIntent fullTable = WriteIntent.builder()
                .overwriteMode(WriteIntent.OverwriteMode.FULL_TABLE).build();
        Assertions.assertTrue(NoopTransactionContext.forIntent(fullTable).label()
                .contains(WriteIntent.OverwriteMode.FULL_TABLE.name()));
    }

    @Test
    public void forIntentRejectsNull() {
        Assertions.assertThrows(NullPointerException.class,
                () -> NoopTransactionContext.forIntent(null));
    }

    @Test
    public void constructorRejectsNullLabel() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new NoopTransactionContext(null));
    }
}
