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

package org.apache.doris.connector.credential;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * BE-dispatchable wrapper around a resolved {@link Credential}.
 *
 * <p>Implements the api-side
 * {@link org.apache.doris.connector.api.credential.BeDispatchableCredential}
 * marker so plugins can receive an instance and pass it to fe-core for thrift
 * dispatch (M1-02). Wraps:
 * <ul>
 *   <li>opaque secret bytes (via {@link Credential})</li>
 *   <li>{@link #expiresAt()} (millis epoch, optional)</li>
 *   <li>{@link #refreshHint()} URI (optional)</li>
 *   <li>{@link Scope} — CATALOG / TABLE / SESSION</li>
 *   <li>{@code beType} — BE-side credential-type identifier</li>
 * </ul>
 *
 * <p>{@link #toString()} MUST never reveal the secret content.</p>
 *
 * <p>The {@code TConnectorCredential} thrift type does not yet exist (M1-02
 * deliverable); a {@code toThrift()} method is intentionally omitted until
 * the thrift surface lands.</p>
 */
public final class BeDispatchableCredential
        implements org.apache.doris.connector.api.credential.BeDispatchableCredential {

    private static final String REDACTED = "***REDACTED***";

    /** Lifetime / fan-out scope for a BE-dispatched credential. */
    public enum Scope {
        CATALOG,
        TABLE,
        SESSION
    }

    private final String beType;
    private final Credential credential;
    private final Scope scope;

    public BeDispatchableCredential(String beType, Credential credential, Scope scope) {
        if (beType == null || beType.isEmpty()) {
            throw new IllegalArgumentException("beType is required");
        }
        if (credential == null) {
            throw new IllegalArgumentException("credential is required");
        }
        if (scope == null) {
            throw new IllegalArgumentException("scope is required");
        }
        this.beType = beType;
        this.credential = credential;
        this.scope = scope;
    }

    @Override
    public String beType() {
        return beType;
    }

    public Credential credential() {
        return credential;
    }

    public Scope scope() {
        return scope;
    }

    public Optional<Long> expiresAt() {
        return credential.expiresAt().map(Instant::toEpochMilli);
    }

    public Optional<URI> refreshHint() {
        return credential.refreshHint();
    }

    public byte[] secretBytes() {
        return credential.secretBytes();
    }

    @Override
    public Map<String, String> serialize() {
        LinkedHashMap<String, String> m = new LinkedHashMap<>();
        m.put("be_type", beType);
        m.put("scope", scope.name());
        m.put("secret", credential.secretAsString());
        credential.expiresAt().ifPresent(t -> m.put("expires_at_ms", String.valueOf(t.toEpochMilli())));
        credential.refreshHint().ifPresent(r -> m.put("refresh_hint", r.toString()));
        return Collections.unmodifiableMap(m);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BeDispatchableCredential)) {
            return false;
        }
        BeDispatchableCredential that = (BeDispatchableCredential) o;
        return beType.equals(that.beType)
                && credential.equals(that.credential)
                && scope == that.scope;
    }

    @Override
    public int hashCode() {
        return Objects.hash(beType, credential, scope);
    }

    @Override
    public String toString() {
        return "BeDispatchableCredential{beType=" + beType
                + ", scope=" + scope
                + ", secret=" + REDACTED
                + ", expiresAt=" + expiresAt()
                + ", refreshHint=" + (credential.refreshHint().isPresent() ? REDACTED : "Optional.empty")
                + '}';
    }
}
