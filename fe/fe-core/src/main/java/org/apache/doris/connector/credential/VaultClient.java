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

/**
 * Pluggable Vault client used by {@link VaultCredentialResolver}.
 * Implementations are typically wired against a real Vault HTTP API; the
 * default {@link VaultCredentialResolver#UNCONFIGURED} stub fails fast when
 * the {@code connector_vault_endpoint} / {@code connector_vault_token} knobs
 * are empty.
 */
public interface VaultClient {

    /**
     * Read the value of {@code field} stored under {@code path}.
     *
     * @throws CredentialResolutionException if the read fails or the field is
     *         missing
     */
    String read(String path, String field);
}
