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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable resolved credential value returned by a {@link CredentialResolver}.
 *
 * <p>Holds the secret as an opaque byte[] so callers don't accidentally
 * intern it in the String pool. {@link #toString()} MUST never reveal the
 * secret content.</p>
 */
public final class Credential {

    private static final String REDACTED = "***REDACTED***";

    private final byte[] secret;
    private final Optional<Instant> expiresAt;
    private final Optional<URI> refreshHint;

    private Credential(byte[] secret, Optional<Instant> expiresAt, Optional<URI> refreshHint) {
        if (secret == null) {
            throw new IllegalArgumentException("secret is required");
        }
        this.secret = secret.clone();
        this.expiresAt = Objects.requireNonNull(expiresAt, "expiresAt");
        this.refreshHint = Objects.requireNonNull(refreshHint, "refreshHint");
    }

    public static Credential ofSecret(String secret) {
        return ofSecret(secret, null, null);
    }

    public static Credential ofSecret(String secret, Instant expiresAt, URI refreshHint) {
        if (secret == null) {
            throw new IllegalArgumentException("secret is required");
        }
        return new Credential(secret.getBytes(StandardCharsets.UTF_8),
                Optional.ofNullable(expiresAt), Optional.ofNullable(refreshHint));
    }

    public static Credential ofBytes(byte[] secret, Instant expiresAt, URI refreshHint) {
        return new Credential(secret, Optional.ofNullable(expiresAt), Optional.ofNullable(refreshHint));
    }

    /** Returns a defensive copy of the raw secret bytes. */
    public byte[] secretBytes() {
        return secret.clone();
    }

    /** Returns the secret as a UTF-8 string. Use sparingly; prefer {@link #secretBytes()}. */
    public String secretAsString() {
        return new String(secret, StandardCharsets.UTF_8);
    }

    public Optional<Instant> expiresAt() {
        return expiresAt;
    }

    public Optional<URI> refreshHint() {
        return refreshHint;
    }

    public boolean isExpired(Instant now) {
        Objects.requireNonNull(now, "now");
        return expiresAt.isPresent() && !now.isBefore(expiresAt.get());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Credential)) {
            return false;
        }
        Credential that = (Credential) o;
        return Arrays.equals(secret, that.secret)
                && expiresAt.equals(that.expiresAt)
                && refreshHint.equals(that.refreshHint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(secret), expiresAt, refreshHint);
    }

    @Override
    public String toString() {
        return "Credential{secret=" + REDACTED
                + ", expiresAt=" + expiresAt
                + ", refreshHint=" + (refreshHint.isPresent() ? REDACTED : "Optional.empty")
                + '}';
    }
}
