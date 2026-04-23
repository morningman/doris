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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable user context used by {@link RuntimeImpersonationOps}. */
public final class UserContext {

    private final String username;
    private final String currentRole;
    private final Set<String> groups;
    private final Map<String, String> attrs;

    private UserContext(Builder b) {
        if (b.username == null || b.username.isEmpty()) {
            throw new IllegalArgumentException("username is required");
        }
        if (b.groups == null) {
            throw new IllegalArgumentException("groups is required (may be empty)");
        }
        if (b.attrs == null) {
            throw new IllegalArgumentException("attrs is required (may be empty)");
        }
        this.username = b.username;
        this.currentRole = b.currentRole;
        this.groups = Collections.unmodifiableSet(new LinkedHashSet<>(b.groups));
        this.attrs = Collections.unmodifiableMap(new LinkedHashMap<>(b.attrs));
    }

    public String username() {
        return username;
    }

    /** May be null when no role is currently active. */
    public String currentRole() {
        return currentRole;
    }

    public Set<String> groups() {
        return groups;
    }

    public Map<String, String> attrs() {
        return attrs;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UserContext)) {
            return false;
        }
        UserContext that = (UserContext) o;
        return username.equals(that.username)
                && Objects.equals(currentRole, that.currentRole)
                && groups.equals(that.groups)
                && attrs.equals(that.attrs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, currentRole, groups, attrs);
    }

    @Override
    public String toString() {
        return "UserContext{username=" + username
                + ", currentRole=" + currentRole
                + ", groups=" + groups
                + ", attrs=" + attrs
                + '}';
    }

    /** Builder for {@link UserContext}. */
    public static final class Builder {
        private String username;
        private String currentRole;
        private Set<String> groups = Collections.emptySet();
        private Map<String, String> attrs = Collections.emptyMap();

        private Builder() {
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder currentRole(String currentRole) {
            this.currentRole = currentRole;
            return this;
        }

        public Builder groups(Set<String> groups) {
            this.groups = groups;
            return this;
        }

        public Builder attrs(Map<String, String> attrs) {
            this.attrs = attrs;
            return this;
        }

        public UserContext build() {
            return new UserContext(this);
        }
    }
}
