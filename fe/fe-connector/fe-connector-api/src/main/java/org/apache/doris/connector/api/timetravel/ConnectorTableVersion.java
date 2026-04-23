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

package org.apache.doris.connector.api.timetravel;

import java.time.Instant;
import java.util.Objects;

/**
 * Identifies a specific version of a table for time-travel reads.
 *
 * <p>Sealed hierarchy modelled as Java-17 records; engine produces one of the
 * permitted subtypes from SQL clauses such as {@code FOR VERSION AS OF},
 * {@code FOR TIMESTAMP AS OF} or {@code FOR BRANCH AS OF}.</p>
 */
public sealed interface ConnectorTableVersion
        permits ConnectorTableVersion.BySnapshotId,
                ConnectorTableVersion.ByTimestamp,
                ConnectorTableVersion.ByRef,
                ConnectorTableVersion.ByRefAtTimestamp,
                ConnectorTableVersion.ByOpaque {

    /** Pin to a specific snapshot id (e.g. Iceberg snapshot id). */
    record BySnapshotId(long snapshotId) implements ConnectorTableVersion {
    }

    /** Pin to the latest snapshot at or before the given instant. */
    record ByTimestamp(Instant ts) implements ConnectorTableVersion {
        public ByTimestamp {
            Objects.requireNonNull(ts, "ts");
        }
    }

    /** Pin to the head of a named branch or tag. */
    record ByRef(String name, RefKind kind) implements ConnectorTableVersion {
        public ByRef {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(kind, "kind");
        }
    }

    /** Pin to a named ref then resolve as of the given instant. */
    record ByRefAtTimestamp(String name, RefKind kind, Instant ts) implements ConnectorTableVersion {
        public ByRefAtTimestamp {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(ts, "ts");
        }
    }

    /** Plugin-specific opaque token (e.g. Hudi instant time). */
    record ByOpaque(String token) implements ConnectorTableVersion {
        public ByOpaque {
            Objects.requireNonNull(token, "token");
        }
    }
}
