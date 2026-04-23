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

/** Immutable principal description used to authenticate against an external metastore. */
public final class MetastorePrincipal {

    private final String type;
    private final String name;
    private final Map<String, String> attrs;

    private MetastorePrincipal(Builder b) {
        if (b.type == null || b.type.isEmpty()) {
            throw new IllegalArgumentException("type is required");
        }
        if (b.name == null || b.name.isEmpty()) {
            throw new IllegalArgumentException("name is required");
        }
        if (b.attrs == null) {
            throw new IllegalArgumentException("attrs is required (may be empty)");
        }
        this.type = b.type;
        this.name = b.name;
        this.attrs = Collections.unmodifiableMap(new LinkedHashMap<>(b.attrs));
    }

    public String type() {
        return type;
    }

    public String name() {
        return name;
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
        if (!(o instanceof MetastorePrincipal)) {
            return false;
        }
        MetastorePrincipal that = (MetastorePrincipal) o;
        return type.equals(that.type) && name.equals(that.name) && attrs.equals(that.attrs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, attrs);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MetastorePrincipal{type=").append(type)
                .append(", name=").append(name)
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

    /** Builder for {@link MetastorePrincipal}. */
    public static final class Builder {
        private String type;
        private String name;
        private Map<String, String> attrs = Collections.emptyMap();

        private Builder() {
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder attrs(Map<String, String> attrs) {
            this.attrs = attrs;
            return this;
        }

        public MetastorePrincipal build() {
            return new MetastorePrincipal(this);
        }
    }
}
