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

package org.apache.doris.connector.api.cache;

/**
 * Predicate used by fe-core to decide whether a given
 * {@link ConnectorMetaCacheBinding} should react to an
 * {@link InvalidateRequest} before propagating it.
 */
@FunctionalInterface
public interface ConnectorMetaCacheInvalidation {

    /**
     * Returns true if this binding is affected by the given invalidate request.
     * Used by fe-core to filter bindings before propagating an InvalidateRequest.
     */
    boolean appliesTo(InvalidateRequest req);

    /** A binding that is invalidated by every request. */
    static ConnectorMetaCacheInvalidation always() {
        return req -> true;
    }

    /** A binding that is invalidated only when scope matches. */
    static ConnectorMetaCacheInvalidation byScope(InvalidateScope scope) {
        return req -> req.getScope() == scope;
    }
}
