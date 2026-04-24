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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** JDBC-credential surface exposed by {@link CredentialBroker}. */
public interface JdbcCredentialOps {

    /**
     * Resolve the JDBC connection properties for {@code req}. The returned
     * {@link Properties} typically contains at least {@code user} and
     * {@code password} entries; the broker may resolve {@code env://},
     * {@code file://}, {@code kms://}, or {@code vault://} references on the
     * caller's behalf.
     */
    Properties getConnectionProperties(JdbcRequest req);

    /**
     * Force-refresh the credential bound to {@code req}. Returns {@code true}
     * if the broker's cache had an entry that was actually invalidated.
     */
    boolean rotateIfNeeded(JdbcRequest req);

    /**
     * Resolve the JDBC credential as a {@link CredentialEnvelope} carrying
     * {@code expiresAt} / {@code refreshHint} metadata for plugin-side
     * caching. The default implementation wraps {@link #getConnectionProperties}
     * with no expiry; broker implementations that resolve through URI
     * resolvers MUST override to propagate the underlying credential's expiry
     * and refresh hint.
     */
    default CredentialEnvelope resolve(JdbcRequest req) {
        Properties p = getConnectionProperties(req);
        Map<String, String> payload = new LinkedHashMap<>();
        for (String name : p.stringPropertyNames()) {
            payload.put(name, p.getProperty(name));
        }
        return CredentialEnvelope.builder()
                .type("jdbc")
                .scope(CredentialScope.CATALOG)
                .payload(payload)
                .build();
    }
}
