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

/**
 * Enumerates the optional capabilities a connector may declare.
 * The planner and execution engine use these to decide which
 * pushdown and write paths are available.
 */
public enum ConnectorCapability {
    SUPPORTS_FILTER_PUSHDOWN,
    SUPPORTS_PROJECTION_PUSHDOWN,
    SUPPORTS_LIMIT_PUSHDOWN,
    SUPPORTS_PARTITION_PRUNING,
    SUPPORTS_INSERT,
    SUPPORTS_DELETE,
    SUPPORTS_UPDATE,
    SUPPORTS_MERGE,
    SUPPORTS_CREATE_TABLE,
    SUPPORTS_MVCC_SNAPSHOT,
    SUPPORTS_METASTORE_EVENTS,
    SUPPORTS_STATISTICS,
    SUPPORTS_VENDED_CREDENTIALS,
    SUPPORTS_ACID_TRANSACTIONS,
    SUPPORTS_TIME_TRAVEL
}
