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

package org.apache.doris.connector.api.event;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Sealed hierarchy of metadata change events emitted by a connector via
 * {@link EventSourceOps}. The 13 record permits cover database / table /
 * partition CRUD, data changes, ref changes, and a vendor escape hatch.
 *
 * <p>Note: the nested {@link RefKind} here (BRANCH / TAG / SNAPSHOT_REF)
 * is intentionally distinct from
 * {@code org.apache.doris.connector.api.timetravel.RefKind} (BRANCH /
 * TAG / UNKNOWN); mapping between the two is an engine concern.</p>
 */
public sealed interface ConnectorMetaChangeEvent
        permits ConnectorMetaChangeEvent.DatabaseCreated,
                ConnectorMetaChangeEvent.DatabaseDropped,
                ConnectorMetaChangeEvent.DatabaseAltered,
                ConnectorMetaChangeEvent.TableCreated,
                ConnectorMetaChangeEvent.TableDropped,
                ConnectorMetaChangeEvent.TableAltered,
                ConnectorMetaChangeEvent.TableRenamed,
                ConnectorMetaChangeEvent.PartitionAdded,
                ConnectorMetaChangeEvent.PartitionDropped,
                ConnectorMetaChangeEvent.PartitionAltered,
                ConnectorMetaChangeEvent.DataChanged,
                ConnectorMetaChangeEvent.RefChanged,
                ConnectorMetaChangeEvent.VendorEvent {

    long eventId();

    Instant eventTime();

    String catalog();

    Optional<String> database();

    Optional<String> table();

    Optional<PartitionSpec> partitionSpec();

    String cause();

    /** Event-side ref kind, distinct from {@code timetravel.RefKind}. */
    enum RefKind { BRANCH, TAG, SNAPSHOT_REF }

    // ---------------------------------------------------------------- DB

    record DatabaseCreated(long eventId, Instant eventTime, String catalog, String db, String cause)
            implements ConnectorMetaChangeEvent {
        public DatabaseCreated {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.empty();
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }

    record DatabaseDropped(long eventId, Instant eventTime, String catalog, String db, String cause)
            implements ConnectorMetaChangeEvent {
        public DatabaseDropped {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.empty();
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }

    record DatabaseAltered(long eventId, Instant eventTime, String catalog, String db, String cause)
            implements ConnectorMetaChangeEvent {
        public DatabaseAltered {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.empty();
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }

    // ---------------------------------------------------------------- Table

    record TableCreated(long eventId, Instant eventTime, String catalog, String db, String tbl, String cause)
            implements ConnectorMetaChangeEvent {
        public TableCreated {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }

    record TableDropped(long eventId, Instant eventTime, String catalog, String db, String tbl, String cause)
            implements ConnectorMetaChangeEvent {
        public TableDropped {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }

    record TableAltered(long eventId, Instant eventTime, String catalog, String db, String tbl, String cause)
            implements ConnectorMetaChangeEvent {
        public TableAltered {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }

    record TableRenamed(long eventId, Instant eventTime, String catalog, String db,
                        String oldName, String newName, String cause)
            implements ConnectorMetaChangeEvent {
        public TableRenamed {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(oldName, "oldName");
            Objects.requireNonNull(newName, "newName");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(oldName);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }

        public String newTableName() {
            return newName;
        }
    }

    // ---------------------------------------------------------------- Partition

    record PartitionAdded(long eventId, Instant eventTime, String catalog, String db, String tbl,
                          PartitionSpec spec, String cause)
            implements ConnectorMetaChangeEvent {
        public PartitionAdded {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(spec, "spec");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.of(spec);
        }
    }

    record PartitionDropped(long eventId, Instant eventTime, String catalog, String db, String tbl,
                            PartitionSpec spec, String cause)
            implements ConnectorMetaChangeEvent {
        public PartitionDropped {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(spec, "spec");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.of(spec);
        }
    }

    record PartitionAltered(long eventId, Instant eventTime, String catalog, String db, String tbl,
                            PartitionSpec spec, String cause)
            implements ConnectorMetaChangeEvent {
        public PartitionAltered {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(spec, "spec");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.of(spec);
        }
    }

    // ---------------------------------------------------------------- Data / Ref

    record DataChanged(long eventId, Instant eventTime, String catalog, String db, String tbl,
                       Optional<Long> snapshotId, Optional<PartitionSpec> spec, String cause)
            implements ConnectorMetaChangeEvent {
        public DataChanged {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(snapshotId, "snapshotId");
            Objects.requireNonNull(spec, "spec");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return spec;
        }
    }

    record RefChanged(long eventId, Instant eventTime, String catalog, String db, String tbl,
                      String refName, RefKind kind, Optional<Long> snapshotId, String cause)
            implements ConnectorMetaChangeEvent {
        public RefChanged {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(refName, "refName");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(snapshotId, "snapshotId");
            Objects.requireNonNull(cause, "cause");
        }

        @Override
        public Optional<String> database() {
            return Optional.of(db);
        }

        @Override
        public Optional<String> table() {
            return Optional.of(tbl);
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }

    // ---------------------------------------------------------------- Vendor escape hatch

    record VendorEvent(long eventId, Instant eventTime, String catalog,
                       Optional<String> db, Optional<String> tbl,
                       String vendor, Map<String, String> attributes, String cause)
            implements ConnectorMetaChangeEvent {
        public VendorEvent(long eventId, Instant eventTime, String catalog,
                           Optional<String> db, Optional<String> tbl,
                           String vendor, Map<String, String> attributes, String cause) {
            Objects.requireNonNull(eventTime, "eventTime");
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(db, "db");
            Objects.requireNonNull(tbl, "tbl");
            Objects.requireNonNull(vendor, "vendor");
            Objects.requireNonNull(attributes, "attributes");
            Objects.requireNonNull(cause, "cause");
            this.eventId = eventId;
            this.eventTime = eventTime;
            this.catalog = catalog;
            this.db = db;
            this.tbl = tbl;
            this.vendor = vendor;
            this.attributes = Collections.unmodifiableMap(new HashMap<>(attributes));
            this.cause = cause;
        }

        @Override
        public Optional<String> database() {
            return db;
        }

        @Override
        public Optional<String> table() {
            return tbl;
        }

        @Override
        public Optional<PartitionSpec> partitionSpec() {
            return Optional.empty();
        }
    }
}
