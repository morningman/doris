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
import java.util.function.Function;

/** {@link CredentialResolver} reading {@code env://VAR_NAME} from {@link System#getenv(String)}. */
public final class EnvCredentialResolver implements CredentialResolver {

    public static final String SCHEME = "env";

    private final Function<String, String> envLookup;

    public EnvCredentialResolver() {
        this(System::getenv);
    }

    /** Test-only constructor. */
    EnvCredentialResolver(Function<String, String> envLookup) {
        if (envLookup == null) {
            throw new IllegalArgumentException("envLookup is required");
        }
        this.envLookup = envLookup;
    }

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Credential resolve(URI ref, ResolverContext ctx) {
        if (ref == null) {
            throw new CredentialResolutionException("env reference is null");
        }
        String name = ref.getAuthority();
        if (name == null || name.isEmpty()) {
            // env://VAR — getAuthority returns VAR; if user wrote env:VAR fall back to schemeSpecificPart
            String ssp = ref.getSchemeSpecificPart();
            if (ssp != null && ssp.startsWith("//")) {
                ssp = ssp.substring(2);
            }
            name = ssp;
        }
        if (name == null || name.trim().isEmpty()) {
            throw new CredentialResolutionException("env reference is missing variable name: " + ref);
        }
        String value = envLookup.apply(name);
        if (value == null) {
            throw new CredentialResolutionException("env variable not set: " + name);
        }
        return Credential.ofSecret(value);
    }
}
