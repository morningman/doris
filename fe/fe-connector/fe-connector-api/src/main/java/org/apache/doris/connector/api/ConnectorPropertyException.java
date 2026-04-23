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

package org.apache.doris.connector.api;

import java.util.Objects;

/**
 * Thrown by {@link PropertyValidator}s and the property resolver when a property
 * value is rejected.
 *
 * <p>Carries a stable machine-readable {@link #getCode() code} (e.g.
 * {@code "AMBIGUOUS_ALIAS"}, {@code "TYPE_MISMATCH"}, {@code "REQUIRED_MISSING"})
 * alongside a human-readable message. Extends {@link DorisConnectorException} so
 * callers can catch the connector exception hierarchy uniformly.</p>
 */
public class ConnectorPropertyException extends DorisConnectorException {

    private final String code;

    public ConnectorPropertyException(String code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code, "code");
    }

    public ConnectorPropertyException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
    }

    /** Stable machine-readable error code. */
    public String getCode() {
        return code;
    }
}
