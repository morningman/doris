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

import java.net.URI;
import java.util.Objects;

/** Immutable HTTP endpoint descriptor used by {@link HttpCredentialOps}. */
public final class HttpEndpoint {

    private final URI url;
    private final String catalog;
    private final String purpose;

    private HttpEndpoint(Builder b) {
        if (b.url == null) {
            throw new IllegalArgumentException("url is required");
        }
        if (b.catalog == null || b.catalog.isEmpty()) {
            throw new IllegalArgumentException("catalog is required");
        }
        if (b.purpose == null || b.purpose.isEmpty()) {
            throw new IllegalArgumentException("purpose is required");
        }
        this.url = b.url;
        this.catalog = b.catalog;
        this.purpose = b.purpose;
    }

    public URI url() {
        return url;
    }

    public String catalog() {
        return catalog;
    }

    public String purpose() {
        return purpose;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HttpEndpoint)) {
            return false;
        }
        HttpEndpoint that = (HttpEndpoint) o;
        return url.equals(that.url) && catalog.equals(that.catalog) && purpose.equals(that.purpose);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, catalog, purpose);
    }

    @Override
    public String toString() {
        return "HttpEndpoint{url=" + url + ", catalog=" + catalog + ", purpose=" + purpose + '}';
    }

    /** Builder for {@link HttpEndpoint}. */
    public static final class Builder {
        private URI url;
        private String catalog;
        private String purpose;

        private Builder() {
        }

        public Builder url(URI url) {
            this.url = url;
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public Builder purpose(String purpose) {
            this.purpose = purpose;
            return this;
        }

        public HttpEndpoint build() {
            return new HttpEndpoint(this);
        }
    }
}
