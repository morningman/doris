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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable descriptor of a connector action. Built via {@link Builder}.
 */
public final class ActionDescriptor {

    /**
     * Simple result-column shape. Actions rarely need rich types, so a
     * tight {@code (name, type)} record is preferred over reusing the
     * heavier {@code ConnectorColumn}.
     */
    public record ResultColumn(String name, ActionArgumentType type) {
        public ResultColumn {
            Objects.requireNonNull(name, "name");
            if (name.isBlank()) {
                throw new IllegalArgumentException("column name must not be blank");
            }
            Objects.requireNonNull(type, "type");
        }
    }

    private final String name;
    private final ConnectorActionOps.Scope scope;
    private final List<ActionArgument> arguments;
    private final List<ResultColumn> resultSchema;
    private final boolean transactional;
    private final boolean blocking;
    private final boolean idempotent;

    private ActionDescriptor(Builder b) {
        Objects.requireNonNull(b.name, "name");
        if (b.name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(b.scope, "scope");
        this.name = b.name;
        this.scope = b.scope;
        this.arguments = Collections.unmodifiableList(new ArrayList<>(b.arguments));
        this.resultSchema = Collections.unmodifiableList(new ArrayList<>(b.resultSchema));
        this.transactional = b.transactional;
        this.blocking = b.blocking;
        this.idempotent = b.idempotent;
    }

    public String name() {
        return name;
    }

    public ConnectorActionOps.Scope scope() {
        return scope;
    }

    public List<ActionArgument> arguments() {
        return arguments;
    }

    public List<ResultColumn> resultSchema() {
        return resultSchema;
    }

    public boolean transactional() {
        return transactional;
    }

    public boolean blocking() {
        return blocking;
    }

    public boolean idempotent() {
        return idempotent;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "ActionDescriptor{name=" + name + ", scope=" + scope
                + ", arguments=" + arguments + ", resultSchema=" + resultSchema
                + ", transactional=" + transactional + ", blocking=" + blocking
                + ", idempotent=" + idempotent + "}";
    }

    /** Builder for {@link ActionDescriptor}. */
    public static final class Builder {
        private String name;
        private ConnectorActionOps.Scope scope;
        private List<ActionArgument> arguments = new ArrayList<>();
        private List<ResultColumn> resultSchema = new ArrayList<>();
        private boolean transactional;
        private boolean blocking;
        private boolean idempotent;

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

        public Builder arguments(List<ActionArgument> arguments) {
            Objects.requireNonNull(arguments, "arguments");
            this.arguments = new ArrayList<>(arguments);
            return this;
        }

        public Builder addArgument(ActionArgument argument) {
            Objects.requireNonNull(argument, "argument");
            this.arguments.add(argument);
            return this;
        }

        public Builder resultSchema(List<ResultColumn> resultSchema) {
            Objects.requireNonNull(resultSchema, "resultSchema");
            this.resultSchema = new ArrayList<>(resultSchema);
            return this;
        }

        public Builder addResultColumn(ResultColumn column) {
            Objects.requireNonNull(column, "column");
            this.resultSchema.add(column);
            return this;
        }

        public Builder transactional(boolean transactional) {
            this.transactional = transactional;
            return this;
        }

        public Builder blocking(boolean blocking) {
            this.blocking = blocking;
            return this;
        }

        public Builder idempotent(boolean idempotent) {
            this.idempotent = idempotent;
            return this;
        }

        public ActionDescriptor build() {
            return new ActionDescriptor(this);
        }
    }
}
