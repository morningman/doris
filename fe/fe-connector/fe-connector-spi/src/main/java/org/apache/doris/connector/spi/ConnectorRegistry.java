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

package org.apache.doris.connector.spi;

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;

import java.util.Optional;
import java.util.Set;

/**
 * Engine-side registry of loaded connectors. Used by host connectors during
 * D10 dispatch to look up the delegate connector identified by a
 * {@link org.apache.doris.connector.api.dispatch.DispatchTarget}.
 *
 * <p>The real implementation is wired by fe-core in M4 alongside
 * {@code DelegatingConnectorMetadata}.</p>
 */
public interface ConnectorRegistry {

    /** Look up a loaded connector by type name. Empty when not loaded. */
    Optional<Connector> lookup(String type, ConnectorContext ctx);

    /** Connector type names currently loaded. */
    Set<String> listLoaded();

    /** Convenience: whether the loaded connector of {@code type} declares {@code cap}. */
    boolean supports(String type, ConnectorCapability cap);
}
