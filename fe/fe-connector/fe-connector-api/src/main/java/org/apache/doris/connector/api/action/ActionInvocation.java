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

package org.apache.doris.connector.api.action;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable action invocation. Session / principal threading is the
 * engine dispatcher's concern and is intentionally not modeled here.
 * Argument values are already type-coerced by the engine before dispatch.
 */
public final class ActionInvocation {

    private final String name;
    private final ConnectorActionOps.Scope scope;
    private final ActionTarget target;
    private final Map<String, Object> arguments;
    private final String invocationId;

    private ActionInvocation(Builder b) {
        Objects.requireNonNull(b.name, "name");
        if (b.name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(b.scope, "scope");
        this.name = b.name;
        this.scope = b.scope;
        this.target = b.target;
        this.arguments = Collections.unmodifiableMap(new LinkedHashMap<>(b.arguments));
        this.invocationId = b.invocationId;
    }

    public String name() {
        return name;
    }

    public ConnectorActionOps.Scope scope() {
        return scope;
    }

    public Optional<ActionTarget> target() {
        return Optional.ofNullable(target);
    }

    public Map<String, Object> arguments() {
        return arguments;
    }

    public Optional<String> invocationId() {
        return Optional.ofNullable(invocationId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "ActionInvocation{name=" + name + ", scope=" + scope
                + ", target=" + Optional.ofNullable(target)
                + ", arguments=" + arguments
                + ", invocationId=" + Optional.ofNullable(invocationId) + "}";
    }

    /** Builder for {@link ActionInvocation}. */
    public static final class Builder {
        private String name;
        private ConnectorActionOps.Scope scope;
        private ActionTarget target;
        private Map<String, Object> arguments = new LinkedHashMap<>();
        private String invocationId;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder scope(ConnectorActionOps.Scope scope) {
            this.scope = scope;
            return this;
        }

        public Builder target(ActionTarget target) {
            this.target = target;
            return this;
        }

        public Builder arguments(Map<String, Object> arguments) {
            Objects.requireNonNull(arguments, "arguments");
            this.arguments = new LinkedHashMap<>(arguments);
            return this;
        }

        public Builder putArgument(String key, Object value) {
            Objects.requireNonNull(key, "key");
            this.arguments.put(key, value);
            return this;
        }

        public Builder invocationId(String invocationId) {
            this.invocationId = invocationId;
            return this;
        }

        public ActionInvocation build() {
            return new ActionInvocation(this);
        }
    }
}
