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

package org.apache.doris.connector.jdbc.credential;

import org.apache.doris.connector.api.credential.BeDispatchableCredential;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * api-level {@link BeDispatchableCredential} marker carrying a resolved JDBC
 * user/password pair. {@code beType} is {@code "JDBC"}; the serialized map
 * contains {@code user}, {@code password} and (optional) {@code expires_at_ms}
 * / {@code refresh_hint}.
 *
 * <p>{@link #toString()} MUST never reveal the password content.</p>
 */
public final class BeDispatchableJdbcCredential implements BeDispatchableCredential {

    private static final String BE_TYPE = "JDBC";
    private static final String REDACTED = "***REDACTED***";

    private final JdbcConnectionCredential credential;

    public BeDispatchableJdbcCredential(JdbcConnectionCredential credential) {
        this.credential = Objects.requireNonNull(credential, "credential");
    }

    @Override
    public String beType() {
        return BE_TYPE;
    }

    public JdbcConnectionCredential credential() {
        return credential;
    }

    @Override
    public Map<String, String> serialize() {
        LinkedHashMap<String, String> m = new LinkedHashMap<>();
        m.put("be_type", BE_TYPE);
        m.put("user", credential.user());
        m.put("password", credential.password());
        credential.expiresAt().ifPresent(t -> m.put("expires_at_ms", String.valueOf(t.toEpochMilli())));
        credential.refreshHint().ifPresent(r -> m.put("refresh_hint", r));
        return Collections.unmodifiableMap(m);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BeDispatchableJdbcCredential)) {
            return false;
        }
        return credential.equals(((BeDispatchableJdbcCredential) o).credential);
    }

    @Override
    public int hashCode() {
        return credential.hashCode();
    }

    @Override
    public String toString() {
        return "BeDispatchableJdbcCredential{beType=" + BE_TYPE
                + ", user=" + (credential.user().isEmpty() ? "" : REDACTED)
                + ", password=" + REDACTED
                + ", expiresAt=" + credential.expiresAt()
                + ", refreshHint=" + (credential.refreshHint().isPresent() ? REDACTED : "Optional.empty")
                + '}';
    }
}
