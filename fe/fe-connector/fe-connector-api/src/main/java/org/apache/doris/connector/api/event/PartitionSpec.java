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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Event-flavored partition specification: a column-name to
 * string-encoded value map, preserving insertion order.
 *
 * <p>This is intentionally decoupled from any pushdown / scan partition
 * descriptor; events carry just enough information to identify the
 * partition involved. Plugins encode values as plain strings so the
 * engine does not need to know the partition column types ahead of
 * time.</p>
 */
public final class PartitionSpec {

    private final Map<String, String> values;

    public PartitionSpec(LinkedHashMap<String, String> values) {
        Objects.requireNonNull(values, "values");
        LinkedHashMap<String, String> copy = new LinkedHashMap<>(values);
        this.values = Collections.unmodifiableMap(copy);
    }

    public Map<String, String> values() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionSpec)) {
            return false;
        }
        PartitionSpec that = (PartitionSpec) o;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "PartitionSpec" + values;
    }
}
