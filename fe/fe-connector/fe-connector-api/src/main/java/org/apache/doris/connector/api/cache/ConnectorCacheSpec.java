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

package org.apache.doris.connector.api.cache;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable specification describing how the fe-core cache registry should
 * configure the cache backing a {@link ConnectorMetaCacheBinding}.
 *
 * <p>Use {@link #builder()} or {@link #defaults()} to obtain instances.</p>
 */
public final class ConnectorCacheSpec {

    private static final long DEFAULT_MAX_SIZE = 10_000L;
    private static final Duration DEFAULT_TTL = Duration.ofHours(1);
    private static final RefreshPolicy DEFAULT_REFRESH_POLICY = RefreshPolicy.TTL;

    private final long maxSize;
    private final Duration ttl;
    private final Duration refreshAfter;
    private final boolean softValues;
    private final RefreshPolicy refreshPolicy;

    private ConnectorCacheSpec(Builder b) {
        if (b.maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be > 0, got " + b.maxSize);
        }
        if (b.ttl == null) {
            throw new IllegalArgumentException("ttl must not be null");
        }
        if (b.refreshPolicy == null) {
            throw new IllegalArgumentException("refreshPolicy must not be null");
        }
        if (b.refreshAfter != null) {
            if (b.refreshPolicy != RefreshPolicy.TTL) {
                throw new IllegalArgumentException(
                        "refreshAfter is only meaningful when refreshPolicy == TTL, got " + b.refreshPolicy);
            }
            if (b.refreshAfter.compareTo(b.ttl) > 0) {
                throw new IllegalArgumentException(
                        "refreshAfter (" + b.refreshAfter + ") must be <= ttl (" + b.ttl + ")");
            }
        }
        this.maxSize = b.maxSize;
        this.ttl = b.ttl;
        this.refreshAfter = b.refreshAfter;
        this.softValues = b.softValues;
        this.refreshPolicy = b.refreshPolicy;
    }

    /** Returns a fresh builder pre-populated with the default values. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns a spec with all default values. */
    public static ConnectorCacheSpec defaults() {
        return new Builder().build();
    }

    public long getMaxSize() {
        return maxSize;
    }

    public Duration getTtl() {
        return ttl;
    }

    /** Returns the refresh-after-write duration, or {@code null} if unset. */
    public Duration getRefreshAfter() {
        return refreshAfter;
    }

    public boolean isSoftValues() {
        return softValues;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorCacheSpec)) {
            return false;
        }
        ConnectorCacheSpec that = (ConnectorCacheSpec) o;
        return maxSize == that.maxSize
                && softValues == that.softValues
                && Objects.equals(ttl, that.ttl)
                && Objects.equals(refreshAfter, that.refreshAfter)
                && refreshPolicy == that.refreshPolicy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxSize, ttl, refreshAfter, softValues, refreshPolicy);
    }

    @Override
    public String toString() {
        return "ConnectorCacheSpec{maxSize=" + maxSize
                + ", ttl=" + ttl
                + ", refreshAfter=" + refreshAfter
                + ", softValues=" + softValues
                + ", refreshPolicy=" + refreshPolicy
                + '}';
    }

    /** Fluent builder for {@link ConnectorCacheSpec}. */
    public static final class Builder {
        private long maxSize = DEFAULT_MAX_SIZE;
        private Duration ttl = DEFAULT_TTL;
        private Duration refreshAfter;
        private boolean softValues;
        private RefreshPolicy refreshPolicy = DEFAULT_REFRESH_POLICY;

        private Builder() {
        }

        public Builder maxSize(long maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder ttl(Duration ttl) {
            this.ttl = ttl;
            return this;
        }

        public Builder refreshAfter(Duration refreshAfter) {
            this.refreshAfter = refreshAfter;
            return this;
        }

        public Builder softValues(boolean softValues) {
            this.softValues = softValues;
            return this;
        }

        public Builder refreshPolicy(RefreshPolicy refreshPolicy) {
            this.refreshPolicy = refreshPolicy;
            return this;
        }

        public ConnectorCacheSpec build() {
            return new ConnectorCacheSpec(this);
        }
    }
}
