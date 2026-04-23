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

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Immutable credential envelope produced by a {@link CredentialBroker}.
 *
 * <p>The {@link #payload()} bag is opaque to fe-core; only the dispatcher that
 * matches {@link #type()} is expected to interpret its contents. {@link #toString()}
 * MUST never reveal payload values — they are masked so that accidental log
 * statements do not leak secrets.
 */
public final class CredentialEnvelope {

    private final String type;
    private final Map<String, String> payload;
    private final CredentialScope scope;
    private final Optional<Instant> expiresAt;
    private final Optional<String> refreshHint;

    private CredentialEnvelope(Builder b) {
        if (b.type == null || b.type.isEmpty()) {
            throw new IllegalArgumentException("type is required");
        }
        if (b.payload == null) {
            throw new IllegalArgumentException("payload is required (may be empty)");
        }
        if (b.scope == null) {
            throw new IllegalArgumentException("scope is required");
        }
        this.type = b.type;
        this.payload = Collections.unmodifiableMap(new LinkedHashMap<>(b.payload));
        this.scope = b.scope;
        this.expiresAt = b.expiresAt == null ? Optional.empty() : b.expiresAt;
        this.refreshHint = b.refreshHint == null ? Optional.empty() : b.refreshHint;
    }

    public String type() {
        return type;
    }

    public Map<String, String> payload() {
        return payload;
    }

    public CredentialScope scope() {
        return scope;
    }

    public Optional<Instant> expiresAt() {
        return expiresAt;
    }

    public Optional<String> refreshHint() {
        return refreshHint;
    }

    /**
     * Returns true if {@link #expiresAt()} is present and not after {@code now}.
     * An envelope without an expiry is treated as never expiring by the FE clock.
     */
    public boolean isExpired(Instant now) {
        Objects.requireNonNull(now, "now");
        return expiresAt.isPresent() && !now.isBefore(expiresAt.get());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CredentialEnvelope)) {
            return false;
        }
        CredentialEnvelope that = (CredentialEnvelope) o;
        return type.equals(that.type)
                && payload.equals(that.payload)
                && scope == that.scope
                && expiresAt.equals(that.expiresAt)
                && refreshHint.equals(that.refreshHint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, payload, scope, expiresAt, refreshHint);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CredentialEnvelope{type=").append(type)
                .append(", scope=").append(scope)
                .append(", payload={");
        boolean first = true;
        for (String k : new TreeSet<>(payload.keySet())) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(k).append("=***");
            first = false;
        }
        sb.append("}, expiresAt=").append(expiresAt)
                .append(", refreshHint=").append(refreshHint.isPresent() ? "***" : "Optional.empty")
                .append('}');
        return sb.toString();
    }

    /** Builder for {@link CredentialEnvelope}. */
    public static final class Builder {
        private String type;
        private Map<String, String> payload;
        private CredentialScope scope;
        private Optional<Instant> expiresAt = Optional.empty();
        private Optional<String> refreshHint = Optional.empty();

        private Builder() {
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder payload(Map<String, String> payload) {
            this.payload = payload;
            return this;
        }

        public Builder scope(CredentialScope scope) {
            this.scope = scope;
            return this;
        }

        public Builder expiresAt(Instant expiresAt) {
            this.expiresAt = Optional.ofNullable(expiresAt);
            return this;
        }

        public Builder refreshHint(String refreshHint) {
            this.refreshHint = Optional.ofNullable(refreshHint);
            return this;
        }

        public CredentialEnvelope build() {
            return new CredentialEnvelope(this);
        }
    }
}
