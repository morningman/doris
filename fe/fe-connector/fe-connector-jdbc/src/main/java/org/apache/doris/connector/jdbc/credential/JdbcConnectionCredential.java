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

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolved JDBC credential pair (user + password) returned by
 * {@link JdbcCredentialContext#getDriverCredential()}.
 *
 * <p>Immutable; {@link #toString()} MUST never reveal the password content.
 * The {@link #expiresAt()} / {@link #refreshHint()} fields propagate the
 * underlying resolver's metadata so the connector cache can respect them.</p>
 */
public final class JdbcConnectionCredential {

    private static final String REDACTED = "***REDACTED***";

    private final String user;
    private final String password;
    private final Optional<Instant> expiresAt;
    private final Optional<String> refreshHint;

    public JdbcConnectionCredential(String user, String password,
                                    Instant expiresAt, String refreshHint) {
        this.user = user == null ? "" : user;
        this.password = password == null ? "" : password;
        this.expiresAt = Optional.ofNullable(expiresAt);
        this.refreshHint = Optional.ofNullable(refreshHint);
    }

    public String user() {
        return user;
    }

    public String password() {
        return password;
    }

    public Optional<Instant> expiresAt() {
        return expiresAt;
    }

    public Optional<String> refreshHint() {
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
        if (!(o instanceof JdbcConnectionCredential)) {
            return false;
        }
        JdbcConnectionCredential that = (JdbcConnectionCredential) o;
        return user.equals(that.user)
                && password.equals(that.password)
                && expiresAt.equals(that.expiresAt)
                && refreshHint.equals(that.refreshHint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, password, expiresAt, refreshHint);
    }

    @Override
    public String toString() {
        return "JdbcConnectionCredential{user=" + (user.isEmpty() ? "" : REDACTED)
                + ", password=" + REDACTED
                + ", expiresAt=" + expiresAt
                + ", refreshHint=" + (refreshHint.isPresent() ? REDACTED : "Optional.empty")
                + '}';
    }
}
