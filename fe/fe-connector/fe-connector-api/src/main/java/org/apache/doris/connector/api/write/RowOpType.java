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
 * Per-row operation kind emitted by the engine write path (D1).
 *
 * <p>Used by connectors that need to distinguish DML row semantics within a
 * single commit (e.g., Iceberg MERGE producing position-deletes, equality-
 * deletes and upserts in one commit).</p>
 */
public enum RowOpType {
    /** Plain INSERT of a new row. */
    INSERT,
    /** Position-based delete (file + row ordinal). */
    POSITION_DELETE,
    /** Equality-based delete (keyed by equality columns). */
    EQUALITY_DELETE,
    /** Deletion-vector encoded delete (bitmap of row positions). */
    DELETION_VECTOR,
    /** The "before" image of an UPDATE, carrying old column values. */
    UPDATE_BEFORE,
    /** The "after" image of an UPDATE, carrying new column values. */
    UPDATE_AFTER
}
