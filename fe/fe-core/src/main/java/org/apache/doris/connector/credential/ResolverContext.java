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

import java.util.Objects;

/**
 * Context passed to {@link CredentialResolver#resolve(java.net.URI, ResolverContext)}.
 *
 * <p>Carries the catalog name so resolvers can apply per-catalog policies and a
 * default cache-TTL hint that resolvers may use when the underlying source
 * does not return its own expiry.</p>
 */
public final class ResolverContext {

    private final String catalogName;
    private final long defaultTtlSeconds;

    public ResolverContext(String catalogName, long defaultTtlSeconds) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        if (defaultTtlSeconds <= 0) {
            throw new IllegalArgumentException("defaultTtlSeconds must be positive");
        }
        this.defaultTtlSeconds = defaultTtlSeconds;
    }

    public String catalogName() {
        return catalogName;
    }

    public long defaultTtlSeconds() {
        return defaultTtlSeconds;
    }
}
