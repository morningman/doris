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
 * Pluggable KMS provider used by {@link KmsCredentialResolver}. Real KMS
 * integrations (AWS KMS, Tencent KMS, Aliyun KMS, ...) are supplied via the
 * {@code connector_kms_provider_class} FQCN config knob.
 */
public interface KmsProvider {

    /**
     * Decrypt or look up a secret keyed by {@code (provider, keyId)}.
     *
     * @throws CredentialResolutionException if the lookup or decryption fails
     */
    String decrypt(String provider, String keyId);
}
