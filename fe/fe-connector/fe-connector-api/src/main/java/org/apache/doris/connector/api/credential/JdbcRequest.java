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
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

/** Immutable JDBC credential request. */
public final class JdbcRequest {

    private final String catalog;
    private final String url;
    private final String username;
    private final Map<String, String> attrs;

    private JdbcRequest(Builder b) {
        if (b.catalog == null || b.catalog.isEmpty()) {
            throw new IllegalArgumentException("catalog is required");
        }
        if (b.url == null || b.url.isEmpty()) {
            throw new IllegalArgumentException("url is required");
        }
        if (b.attrs == null) {
            throw new IllegalArgumentException("attrs is required (may be empty)");
        }
        this.catalog = b.catalog;
        this.url = b.url;
        this.username = b.username;
        this.attrs = Collections.unmodifiableMap(new LinkedHashMap<>(b.attrs));
    }

    public String catalog() {
        return catalog;
    }

    public String url() {
        return url;
    }

    /** May be null when the credential broker should derive the username. */
    public String username() {
        return username;
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
        if (!(o instanceof JdbcRequest)) {
            return false;
        }
        JdbcRequest that = (JdbcRequest) o;
        return catalog.equals(that.catalog)
                && url.equals(that.url)
                && Objects.equals(username, that.username)
                && attrs.equals(that.attrs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, url, username, attrs);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("JdbcRequest{catalog=").append(catalog)
                .append(", url=").append(url)
                .append(", username=").append(username == null ? "null" : "***")
                .append(", attrs={");
        boolean first = true;
        for (String k : new TreeSet<>(attrs.keySet())) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(k).append("=***");
            first = false;
        }
        sb.append("}}");
        return sb.toString();
    }

    /** Builder for {@link JdbcRequest}. */
    public static final class Builder {
        private String catalog;
        private String url;
        private String username;
        private Map<String, String> attrs = Collections.emptyMap();

        private Builder() {
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder attrs(Map<String, String> attrs) {
            this.attrs = attrs;
            return this;
        }

        public JdbcRequest build() {
            return new JdbcRequest(this);
        }
    }
}
