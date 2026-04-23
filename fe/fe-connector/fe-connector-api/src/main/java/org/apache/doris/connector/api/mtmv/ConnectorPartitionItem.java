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

package org.apache.doris.connector.api.mtmv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A single partition descriptor returned from
 * {@link MtmvOps#listPartitions}. The MTMV planner does not interpret these
 * representations directly — it relies on {@link #serialized()} as a stable
 * deterministic key for change detection across snapshots.
 */
public sealed interface ConnectorPartitionItem
        permits ConnectorPartitionItem.RangePartitionItem,
                ConnectorPartitionItem.ListPartitionItem,
                ConnectorPartitionItem.UnpartitionedItem {

    /** Stable plugin-defined string suitable for equality / change detection. */
    String serialized();

    /**
     * Half-open range {@code [lowerBound, upperBound)} expressed as plugin-side
     * literal strings. Either bound may be {@code "MIN"} / {@code "MAX"} to
     * denote unbounded edges; the engine treats the value opaquely.
     */
    record RangePartitionItem(String lowerBound, String upperBound) implements ConnectorPartitionItem {
        public RangePartitionItem {
            Objects.requireNonNull(lowerBound, "lowerBound");
            Objects.requireNonNull(upperBound, "upperBound");
        }

        @Override
        public String serialized() {
            return "RANGE[" + lowerBound + "," + upperBound + ")";
        }
    }

    /**
     * Multi-row, multi-column list partition. {@code values.get(i)} is the
     * i-th tuple of column values. Inner lists must match the connector's
     * declared partition column count.
     */
    record ListPartitionItem(List<List<String>> values) implements ConnectorPartitionItem {
        public ListPartitionItem(List<List<String>> values) {
            Objects.requireNonNull(values, "values");
            List<List<String>> copy = new ArrayList<>(values.size());
            for (List<String> tuple : values) {
                Objects.requireNonNull(tuple, "tuple");
                copy.add(Collections.unmodifiableList(new ArrayList<>(tuple)));
            }
            this.values = Collections.unmodifiableList(copy);
        }

        @Override
        public String serialized() {
            StringBuilder sb = new StringBuilder("LIST[");
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    sb.append(';');
                }
                sb.append(String.join(",", values.get(i)));
            }
            sb.append(']');
            return sb.toString();
        }
    }

    /** Singleton-shaped record for unpartitioned tables. */
    record UnpartitionedItem() implements ConnectorPartitionItem {
        @Override
        public String serialized() {
            return "UNPARTITIONED";
        }
    }
}
