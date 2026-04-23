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

package org.apache.doris.connector.api.credential;

import java.util.Map;

/**
 * Marker for credentials that can be serialized into a BE-thrift dispatchable
 * form. {@link #beType()} is a {@link String} (rather than a thrift enum) so
 * that the api module stays independent of {@code TConnectorCredentialType};
 * the M3 wiring in fe-core is responsible for mapping the string to the stable
 * BE enum name.
 */
public interface BeDispatchableCredential {
    /**
     * Identifier matching a BE {@code TConnectorCredentialType} ordinal/name
     * when wired in M3.
     */
    String beType();

    Map<String, String> serialize();
}
