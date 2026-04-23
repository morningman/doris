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

package org.apache.doris.connector.api.action;

import java.util.List;
import java.util.Optional;

/**
 * Optional interface for connectors that expose named actions / procedures
 * (e.g. {@code iceberg.rewrite_data_files}, {@code paimon.compact},
 * {@code hudi.clean}).
 *
 * <p>Engine dispatches via the CALL syntax:
 * {@code CALL <catalog>.<db>.<table>.<action>(...)}. fe-core resolves the
 * action, validates arguments against the {@link ActionDescriptor}, then
 * invokes {@link #executeAction(ActionInvocation)}.</p>
 *
 * <p>Only connectors that declare {@code SUPPORTS_PROCEDURES} are expected
 * to implement this surface.</p>
 */
public interface ConnectorActionOps {

    /** Scope at which actions can be invoked. */
    enum Scope { CATALOG, SCHEMA, TABLE }

    /**
     * Lists actions visible at {@code scope}. When {@code scope == TABLE},
     * {@code target} carries a {@code (database, table)} pair; for
     * {@code SCHEMA} it carries a {@code (database, empty)} pair; for
     * {@code CATALOG} it may be {@link Optional#empty()}.
     */
    List<ActionDescriptor> listActions(Scope scope, Optional<ActionTarget> target);

    /**
     * Executes an action. Implementations MUST be idempotent when the
     * descriptor's {@link ActionDescriptor#idempotent()} flag is {@code true}.
     */
    ActionResult executeAction(ActionInvocation invocation);
}
