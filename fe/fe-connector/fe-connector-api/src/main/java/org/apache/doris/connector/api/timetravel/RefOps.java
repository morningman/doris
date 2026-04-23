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

import java.util.List;
import java.util.Set;

/**
 * Branch/tag (ref) operations exposed by a connector.
 *
 * <p>Plugins should report only the {@link RefKind}s they actually support via
 * {@link #supportedRefKinds()}; engine is expected to reject mutations of
 * unsupported kinds upstream of the SPI call.</p>
 *
 * <p>NOTE: identifiers are passed as {@code (database, table)} string pairs
 * pending introduction of a typed {@code ConnectorTableId}; overloads taking
 * the typed id will be added when that lands.</p>
 */
public interface RefOps {

    /** Set of ref kinds the connector understands. */
    Set<RefKind> supportedRefKinds();

    /** Lists the refs currently known for the given table. */
    List<ConnectorRef> listRefs(String database, String table);

    /** Creates or replaces a ref on the given table per the supplied mutation. */
    void createOrReplaceRef(String database, String table, ConnectorRefMutation mutation);

    /** Drops the named ref of the given kind on the given table. */
    void dropRef(String database, String table, String name, RefKind kind);
}
