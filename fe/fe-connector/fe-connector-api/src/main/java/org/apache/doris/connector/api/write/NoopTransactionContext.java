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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Default {@link ConnectorTransactionContext} returned by
 * {@link org.apache.doris.connector.api.ConnectorWriteOps#beginTransaction}
 * when a connector does not (yet) opt into the new commit-model SPI.
 *
 * <p>Carries an opaque random {@code txnId}, an empty capability set,
 * and refuses to serialize. Connectors that want any of
 * {@link ConnectorTxnCapability#MULTI_STATEMENT},
 * {@link ConnectorTxnCapability#COMMIT_RETRY} or
 * {@link ConnectorTxnCapability#FAILOVER_SAFE} MUST return their own
 * implementation instead.</p>
 */
public final class NoopTransactionContext implements ConnectorTransactionContext {

    private static final long serialVersionUID = 1L;

    private final String txnId;
    private final String label;

    public NoopTransactionContext(String label) {
        this.txnId = "noop-" + UUID.randomUUID();
        this.label = Objects.requireNonNull(label, "label");
    }

    /** Convenience factory that derives the label from the supplied intent. */
    public static NoopTransactionContext forIntent(WriteIntent intent) {
        Objects.requireNonNull(intent, "intent");
        return new NoopTransactionContext("noop-intent-" + intent.overwriteMode().name());
    }

    @Override
    public String txnId() {
        return txnId;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public byte[] serialize() {
        throw new UnsupportedOperationException(
                "NoopTransactionContext is not failover-safe and cannot be serialized");
    }

    @Override
    public boolean supportsFailover() {
        return false;
    }

    @Override
    public Set<ConnectorTxnCapability> txnCapabilities() {
        return Collections.emptySet();
    }
}
