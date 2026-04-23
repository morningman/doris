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

package org.apache.doris.connector.api.cache;

/**
 * Scope describing which entries an {@link InvalidateRequest} should affect.
 */
public enum InvalidateScope {
    /** All bindings of the catalog (REFRESH CATALOG full). */
    CATALOG,
    /** All bindings whose key includes the given database. */
    DATABASE,
    /** All bindings keyed on a specific table. */
    TABLE,
    /** Partition list / partition meta of a specific table. */
    PARTITIONS,
    /** Sys-table caches of a specific table. */
    SYS_TABLE
}
