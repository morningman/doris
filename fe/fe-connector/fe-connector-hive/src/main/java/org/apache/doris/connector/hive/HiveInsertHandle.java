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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.handle.ConnectorInsertHandle;

import java.util.Objects;

/**
 * Plugin-side {@link ConnectorInsertHandle} for Hive INSERT / INSERT
 * OVERWRITE. Wraps a {@link HiveAcidContext}; non-ACID INSERTs carry
 * a context with {@link HiveAcidContext#isAcid()} = {@code false}.
 */
public final class HiveInsertHandle implements ConnectorInsertHandle {

    private final HiveAcidContext context;

    public HiveInsertHandle(HiveAcidContext context) {
        this.context = Objects.requireNonNull(context, "context");
    }

    public HiveAcidContext getContext() {
        return context;
    }
}
