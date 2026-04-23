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

/**
 * {@link CredentialResolver} for {@code vault://path?field=...}. The actual
 * Vault read is delegated to a pluggable {@link VaultClient}; the default
 * {@link #UNCONFIGURED} client throws when the endpoint / token config is not
 * wired so misconfigured deployments fail loudly.
 */
public final class VaultCredentialResolver implements CredentialResolver {

    public static final String SCHEME = "vault";

    public static final VaultClient UNCONFIGURED = (path, field) -> {
        throw new CredentialResolutionException(
                "vault client not configured "
                        + "(set connector_vault_endpoint and connector_vault_token)");
    };

    private final VaultClient client;

    public VaultCredentialResolver() {
        this(UNCONFIGURED);
    }

    public VaultCredentialResolver(VaultClient client) {
        if (client == null) {
            throw new IllegalArgumentException("client is required");
        }
        this.client = client;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Credential resolve(URI ref, ResolverContext ctx) {
        if (ref == null) {
            throw new CredentialResolutionException("vault reference is null");
        }
        StringBuilder pathBuilder = new StringBuilder();
        if (ref.getAuthority() != null) {
            pathBuilder.append(ref.getAuthority());
        }
        if (ref.getPath() != null) {
            pathBuilder.append(ref.getPath());
        }
        String path = pathBuilder.toString();
        if (path.trim().isEmpty()) {
            throw new CredentialResolutionException("vault reference is missing path: " + ref);
        }
        String field = parseField(ref.getQuery());
        if (field == null || field.trim().isEmpty()) {
            throw new CredentialResolutionException(
                    "vault reference is missing 'field' query parameter: " + ref);
        }
        String secret = client.read(path, field);
        if (secret == null) {
            throw new CredentialResolutionException(
                    "vault client returned null for ref: " + ref);
        }
        return Credential.ofSecret(secret);
    }

    private static String parseField(String query) {
        if (query == null) {
            return null;
        }
        for (String part : query.split("&")) {
            int eq = part.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String k = part.substring(0, eq);
            if ("field".equals(k)) {
                return part.substring(eq + 1);
            }
        }
        return null;
    }
}
