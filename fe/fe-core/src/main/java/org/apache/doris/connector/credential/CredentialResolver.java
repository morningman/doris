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
 * fe-core-internal resolver SPI driving {@link DefaultCredentialBroker}'s
 * resolver chain. NOT exposed in fe-connector-api/spi: external connectors
 * are not allowed to plug in raw resolvers; instead they go through the
 * api-side {@link org.apache.doris.connector.api.credential.CredentialBroker}.
 */
public interface CredentialResolver {

    /** Lower-cased URI scheme (e.g. {@code env}, {@code file}, {@code kms}, {@code vault}). */
    String scheme();

    /**
     * Resolve {@code ref} into a {@link Credential}.
     *
     * @throws CredentialResolutionException if the reference is malformed,
     *         missing, or the underlying source refused.
     */
    Credential resolve(URI ref, ResolverContext ctx);
}
