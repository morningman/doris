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
 * Flags a connector reports via
 * {@link org.apache.doris.connector.api.ConnectorWriteOps#txnCapabilities()}
 * to describe its transaction-layer features (D1).
 */
public enum ConnectorTxnCapability {
    /** Supports multiple DML statements in one transaction. */
    MULTI_STATEMENT,
    /** Supports named savepoints inside a transaction. */
    SAVEPOINT,
    /** Distinguishes retryable commit failures via {@link RetryableCommitException}. */
    COMMIT_RETRY,
    /** Transaction context can be reconstructed after a coordinator failover. */
    FAILOVER_SAFE
}
