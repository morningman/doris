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
 * Strategy a connector uses to realize a DELETE (or the delete portion of a
 * MERGE/UPDATE) in storage (D1).
 */
public enum DeleteMode {
    /** Copy-on-write: rewrite affected data files without any delete file. */
    COW,
    /** Merge-on-read with position-delete files (file path + row ordinal). */
    POSITION_DELETE,
    /** Merge-on-read with equality-delete files (keyed by equality columns). */
    EQUALITY_DELETE,
    /** Merge-on-read with deletion-vector bitmaps. */
    DELETION_VECTOR,
    /** Hive ACID delta directory layout (base / delta / delete_delta). */
    HIVE_ACID_DELTA
}
