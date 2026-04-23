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

/**
 * Opaque, plugin-owned transaction context produced when a write operation
 * begins (D1, design doc §8.1).
 *
 * <p>Reserved for the future commit-model refactor PR; no {@code
 * ConnectorWriteOps} method returns it yet.</p>
 */
public interface ConnectorTransactionContext extends java.io.Serializable {
    /** Returns the plugin-owned transaction id (stable for the lifetime of the txn). */
    String txnId();

    /** Returns the engine-visible label for this transaction (e.g., for EXPLAIN / logs). */
    String label();

    /** Serializes the context for FE→BE transport or failover persistence. */
    byte[] serialize();

    /**
     * Returns {@code true} if this context can be safely reconstructed after a
     * coordinator failover from the bytes returned by {@link #serialize()}.
     */
    boolean supportsFailover();
}
