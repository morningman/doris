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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Mutable HTTP request descriptor. Unlike most types in this package this is
 * intentionally <strong>not</strong> immutable: {@link HttpCredentialOps#sign(HttpRequestSpec, CredentialEnvelope)}
 * mutates this object in-place to attach authentication headers.
 */
public final class HttpRequestSpec {

    private URI url;
    private String method;
    private final Map<String, String> headers;
    private Optional<byte[]> body;

    public HttpRequestSpec(URI url) {
        this(url, "GET");
    }

    public HttpRequestSpec(URI url, String method) {
        this.url = Objects.requireNonNull(url, "url");
        this.method = Objects.requireNonNull(method, "method");
        this.headers = new LinkedHashMap<>();
        this.body = Optional.empty();
    }

    public URI getUrl() {
        return url;
    }

    public void setUrl(URI url) {
        this.url = Objects.requireNonNull(url, "url");
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = Objects.requireNonNull(method, "method");
    }

    /** Returns the live, mutable header map. */
    public Map<String, String> getHeaders() {
        return headers;
    }

    public Optional<byte[]> getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = Optional.ofNullable(body);
    }
}
