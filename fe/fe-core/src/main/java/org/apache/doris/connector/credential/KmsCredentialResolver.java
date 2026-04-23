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
 * {@link CredentialResolver} for {@code kms://provider/keyId}. The actual
 * decryption is delegated to a pluggable {@link KmsProvider}; the default
 * provider is a stub that throws so misconfigured deployments fail loudly
 * rather than silently returning empty material.
 */
public final class KmsCredentialResolver implements CredentialResolver {

    public static final String SCHEME = "kms";

    /** Stub provider used when {@code connector_kms_provider_class} is empty. */
    public static final KmsProvider UNCONFIGURED = (provider, keyId) -> {
        throw new CredentialResolutionException(
                "kms provider not configured (set connector_kms_provider_class)");
    };

    private final KmsProvider provider;

    public KmsCredentialResolver() {
        this(UNCONFIGURED);
    }

    public KmsCredentialResolver(KmsProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException("provider is required");
        }
        this.provider = provider;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Credential resolve(URI ref, ResolverContext ctx) {
        if (ref == null) {
            throw new CredentialResolutionException("kms reference is null");
        }
        String providerName = ref.getAuthority();
        if (providerName == null || providerName.trim().isEmpty()) {
            throw new CredentialResolutionException("kms reference is missing provider: " + ref);
        }
        String path = ref.getPath();
        if (path == null || path.length() <= 1) {
            throw new CredentialResolutionException("kms reference is missing keyId: " + ref);
        }
        String keyId = path.startsWith("/") ? path.substring(1) : path;
        if (keyId.trim().isEmpty()) {
            throw new CredentialResolutionException("kms reference is missing keyId: " + ref);
        }
        String secret = provider.decrypt(providerName, keyId);
        if (secret == null) {
            throw new CredentialResolutionException(
                    "kms provider returned null for ref: " + ref);
        }
        return Credential.ofSecret(secret);
    }
}
