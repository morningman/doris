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

import java.util.Set;

/**
 * Validates a typed property value supplied for a {@link ConnectorPropertyMetadata}.
 *
 * <p>Validators are invoked by the resolver after type conversion and may consult
 * other property values via {@link ValidationContext} to perform cross-key checks.
 * On rejection a validator throws {@link ConnectorPropertyException}.</p>
 *
 * @param <T> the value type produced by the resolver for this property
 */
@FunctionalInterface
public interface PropertyValidator<T> {

    /**
     * Validates {@code value} for the property identified by {@code key}.
     *
     * @throws ConnectorPropertyException if the value is invalid
     */
    void validate(String key, T value, ValidationContext ctx);

    /**
     * Context exposed to a {@link PropertyValidator} during validation.
     *
     * <p>This is a placeholder interface; the fe-core resolver (M0-04) injects the
     * concrete implementation that exposes the full property bag being validated.</p>
     */
    interface ValidationContext {

        /** All property keys currently being validated (post-alias-resolution). */
        Set<String> allKeys();

        /** Raw user-supplied string for {@code key} prior to type conversion. */
        String rawValue(String key);
    }
}
