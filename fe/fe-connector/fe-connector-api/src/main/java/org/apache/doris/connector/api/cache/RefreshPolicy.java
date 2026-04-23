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
 * Refresh policy applied to a {@link ConnectorMetaCacheBinding} by the
 * fe-core-managed cache registry.
 */
public enum RefreshPolicy {
    /** Always reload on every read; bypasses TTL. Use for sys-table-style live data. */
    ALWAYS_REFRESH,
    /** Standard time-to-live with optional refreshAfterWrite (defined in ConnectorCacheSpec). */
    TTL,
    /** Cache holds entries indefinitely until explicit invalidate() / event-driven refresh. */
    MANUAL_ONLY
}
