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

import java.util.Objects;
import java.util.Optional;

/**
 * Describes one formal argument of an action. String-encoded default
 * value is parsed per type by the plugin; the engine performs coercion
 * before dispatch.
 */
public final class ActionArgument {

    private final String name;
    private final ActionArgumentType type;
    private final boolean required;
    private final String defaultValue;

    public ActionArgument(String name, ActionArgumentType type, boolean required, String defaultValue) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        Objects.requireNonNull(type, "type");
        this.name = name;
        this.type = type;
        this.required = required;
        this.defaultValue = defaultValue;
    }

    public ActionArgument(String name, ActionArgumentType type, boolean required) {
        this(name, type, required, null);
    }

    public String name() {
        return name;
    }

    public ActionArgumentType type() {
        return type;
    }

    public boolean required() {
        return required;
    }

    public Optional<String> defaultValue() {
        return Optional.ofNullable(defaultValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionArgument)) {
            return false;
        }
        ActionArgument other = (ActionArgument) o;
        return required == other.required
                && name.equals(other.name)
                && type == other.type
                && Objects.equals(defaultValue, other.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, required, defaultValue);
    }

    @Override
    public String toString() {
        return "ActionArgument{name=" + name + ", type=" + type
                + ", required=" + required + ", defaultValue=" + Optional.ofNullable(defaultValue) + "}";
    }
}
