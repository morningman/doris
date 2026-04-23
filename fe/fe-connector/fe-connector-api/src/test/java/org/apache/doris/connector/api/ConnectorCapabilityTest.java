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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards the ordinal stability of {@link ConnectorCapability}.
 *
 * <p>The enum is persisted by ordinal in edit logs / catalog metadata, so
 * existing values must never be reordered or removed. New values must only
 * be appended at the end.</p>
 */
public class ConnectorCapabilityTest {

    /** The first 17 enum values, in their original (frozen) order. */
    private static final String[] BASELINE_NAMES = new String[] {
            "SUPPORTS_FILTER_PUSHDOWN",
            "SUPPORTS_PROJECTION_PUSHDOWN",
            "SUPPORTS_LIMIT_PUSHDOWN",
            "SUPPORTS_PARTITION_PRUNING",
            "SUPPORTS_INSERT",
            "SUPPORTS_DELETE",
            "SUPPORTS_UPDATE",
            "SUPPORTS_MERGE",
            "SUPPORTS_CREATE_TABLE",
            "SUPPORTS_MVCC_SNAPSHOT",
            "SUPPORTS_METASTORE_EVENTS",
            "SUPPORTS_STATISTICS",
            "SUPPORTS_VENDED_CREDENTIALS",
            "SUPPORTS_ACID_TRANSACTIONS",
            "SUPPORTS_TIME_TRAVEL",
            "SUPPORTS_PARALLEL_WRITE",
            "SUPPORTS_PASSTHROUGH_QUERY",
    };

    /** Names of the 30 reserved values appended by M0-01, in declared order. */
    private static final String[] M0_01_RESERVED_NAMES = new String[] {
            "SUPPORTS_INSERT_OVERWRITE",
            "SUPPORTS_PARTITION_OVERWRITE",
            "SUPPORTS_DYNAMIC_PARTITION_INSERT",
            "SUPPORTS_TXN_INSERT",
            "SUPPORTS_UPSERT",
            "SUPPORTS_ROW_LEVEL_DELETE",
            "SUPPORTS_POSITION_DELETE",
            "SUPPORTS_EQUALITY_DELETE",
            "SUPPORTS_DELETION_VECTOR",
            "SUPPORTS_MERGE_INTO",
            "SUPPORTS_PROCEDURES",
            "SUPPORTS_BRANCH_TAG",
            "SUPPORTS_TIME_TRAVEL_WRITE",
            "SUPPORTS_FAILOVER_SAFE_TXN",
            "SUPPORTS_SYSTEM_TABLES",
            "SUPPORTS_NATIVE_SYS_TABLES",
            "SUPPORTS_TVF_SYS_TABLES",
            "SUPPORTS_PULL_EVENTS",
            "SUPPORTS_PUSH_EVENTS",
            "REQUIRES_SELF_MANAGED_EVENT_LOOP",
            "SUPPORTS_PER_TABLE_FILTER",
            "EMITS_REF_EVENTS",
            "EMITS_DATA_CHANGED_WITH_SNAPSHOT",
            "SUPPORTS_MTMV",
            "SUPPORTS_RLS_HINT",
            "SUPPORTS_MASK_HINT",
            "EMITS_AUDIT_EVENTS",
            "EMITS_DELEGATABLE_TABLES",
            "ACCEPTS_DELEGATION_FROM_HMS",
            "SUPPORTS_DELEGATING_LISTING",
    };

    @Test
    public void totalCountIsStable() {
        Assertions.assertEquals(47, ConnectorCapability.values().length,
                "ConnectorCapability size changed: 17 baseline + 30 M0-01 reserved == 47. "
                        + "If you intentionally added/removed values, update this test and "
                        + "remember edit-log ordinal stability rules.");
    }

    @Test
    public void baselineOrdinalsAreFrozen() {
        ConnectorCapability[] values = ConnectorCapability.values();
        for (int i = 0; i < BASELINE_NAMES.length; i++) {
            Assertions.assertEquals(BASELINE_NAMES[i], values[i].name(),
                    "Ordinal " + i + " of ConnectorCapability changed; existing values "
                            + "must NOT be reordered or removed (persisted in edit log).");
        }
    }

    @Test
    public void m0ReservedOrdinalsAreFrozen() {
        ConnectorCapability[] values = ConnectorCapability.values();
        for (int i = 0; i < M0_01_RESERVED_NAMES.length; i++) {
            int ordinal = BASELINE_NAMES.length + i;
            Assertions.assertEquals(M0_01_RESERVED_NAMES[i], values[ordinal].name(),
                    "Ordinal " + ordinal + " of ConnectorCapability changed; M0-01 reserved "
                            + "values must NOT be reordered.");
        }
    }
}
