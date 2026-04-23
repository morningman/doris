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

import org.apache.doris.common.Config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

/**
 * Builds a {@link DefaultCredentialBroker} from {@link Config} knobs:
 * <ul>
 *   <li>{@code connector_credential_resolver_chain} — comma-separated scheme list
 *       (default {@code env,file,kms,vault})</li>
 *   <li>{@code connector_credential_cache_ttl_sec} — fallback TTL when a
 *       resolved {@link Credential} has no {@code expiresAt}</li>
 *   <li>{@code connector_kms_provider_class} — FQCN of a {@link KmsProvider}
 *       implementation; empty means use the {@link KmsCredentialResolver#UNCONFIGURED}
 *       stub</li>
 *   <li>{@code connector_vault_endpoint} / {@code connector_vault_token} — when
 *       both are non-empty a real {@link VaultClient} would be plugged in
 *       (M1-02 follow-up); empty means use the {@link VaultCredentialResolver#UNCONFIGURED}
 *       stub</li>
 * </ul>
 */
public final class DefaultCredentialBrokerFactory {

    private DefaultCredentialBrokerFactory() {
    }

    public static DefaultCredentialBroker forCatalog(String catalogName) {
        List<String> chain = parseChain(Config.connector_credential_resolver_chain);
        long ttl = Config.connector_credential_cache_ttl_sec;
        KmsProvider kmsProvider = loadKmsProvider(Config.connector_kms_provider_class);
        VaultClient vaultClient = loadVaultClient(
                Config.connector_vault_endpoint, Config.connector_vault_token);

        List<CredentialResolver> resolvers = new ArrayList<>(chain.size());
        for (String scheme : chain) {
            resolvers.add(buildResolver(scheme, kmsProvider, vaultClient));
        }
        return new DefaultCredentialBroker(catalogName, resolvers, ttl);
    }

    static List<String> parseChain(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            List<String> defaults = new ArrayList<>(4);
            defaults.add(EnvCredentialResolver.SCHEME);
            defaults.add(FileCredentialResolver.SCHEME);
            defaults.add(KmsCredentialResolver.SCHEME);
            defaults.add(VaultCredentialResolver.SCHEME);
            return Collections.unmodifiableList(defaults);
        }
        LinkedHashSet<String> dedup = new LinkedHashSet<>();
        for (String token : raw.split(",")) {
            String t = token.trim().toLowerCase(Locale.ROOT);
            if (!t.isEmpty()) {
                dedup.add(t);
            }
        }
        if (dedup.isEmpty()) {
            throw new CredentialResolutionException(
                    "connector_credential_resolver_chain has no valid entries: " + raw);
        }
        return Collections.unmodifiableList(new ArrayList<>(dedup));
    }

    static CredentialResolver buildResolver(String scheme,
                                            KmsProvider kmsProvider,
                                            VaultClient vaultClient) {
        switch (scheme) {
            case "env":
                return new EnvCredentialResolver();
            case "file":
                return new FileCredentialResolver();
            case "kms":
                return new KmsCredentialResolver(kmsProvider);
            case "vault":
                return new VaultCredentialResolver(vaultClient);
            default:
                throw new CredentialResolutionException(
                        "unknown credential resolver scheme: " + scheme);
        }
    }

    static KmsProvider loadKmsProvider(String fqcn) {
        if (fqcn == null || fqcn.trim().isEmpty()) {
            return KmsCredentialResolver.UNCONFIGURED;
        }
        try {
            Class<?> cls = Class.forName(fqcn.trim());
            Object instance = cls.getDeclaredConstructor().newInstance();
            if (!(instance instanceof KmsProvider)) {
                throw new CredentialResolutionException(
                        "connector_kms_provider_class is not a KmsProvider: " + fqcn);
            }
            return (KmsProvider) instance;
        } catch (ReflectiveOperationException e) {
            throw new CredentialResolutionException(
                    "failed to instantiate KmsProvider " + fqcn + ": " + e.getMessage(), e);
        }
    }

    static VaultClient loadVaultClient(String endpoint, String token) {
        if (endpoint == null || endpoint.trim().isEmpty()
                || token == null || token.trim().isEmpty()) {
            return VaultCredentialResolver.UNCONFIGURED;
        }
        // Real HTTP-backed VaultClient is M1-02 follow-up; for now fall through
        // to the stub even when knobs are set, so misconfiguration is loud and
        // there is no half-wired client silently swallowing reads.
        return VaultCredentialResolver.UNCONFIGURED;
    }
}
