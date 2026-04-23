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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Describes a configuration property that a connector exposes.
 *
 * <p>v2 model: prefer {@link #builder(String, PropertyValueType)} to construct instances.
 * The original v1 static factories ({@link #stringProperty}, {@link #intProperty},
 * {@link #booleanProperty}, {@link #requiredStringProperty}) remain as
 * back-compatible wrappers and produce metadata with {@code scope = CATALOG}.</p>
 *
 * @param <T> the Java type of the property value
 */
public final class ConnectorPropertyMetadata<T> {

    private final String name;
    private final String description;
    private final Class<T> type;
    private final T defaultValue;
    private final boolean required;

    private final PropertyValueType valueType;
    private final List<String> aliases;
    private final boolean sensitive;
    private final boolean hidden;
    private final Optional<Deprecation> deprecated;
    private final boolean experimental;
    private final PropertyScope scope;
    private final Optional<PropertyValidator<T>> validator;
    private final Optional<List<String>> enumValues;

    private ConnectorPropertyMetadata(Builder<T> b) {
        this.name = Objects.requireNonNull(b.name, "name");
        this.description = b.description == null ? "" : b.description;
        this.valueType = Objects.requireNonNull(b.valueType, "valueType");
        @SuppressWarnings("unchecked")
        Class<T> javaType = (Class<T>) b.valueType.javaType();
        this.type = javaType;
        this.defaultValue = b.defaultValue;
        this.required = b.required;
        this.aliases = b.aliases == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(b.aliases));
        this.sensitive = b.sensitive;
        this.hidden = b.hidden;
        this.deprecated = Optional.ofNullable(b.deprecated);
        this.experimental = b.experimental;
        this.scope = b.scope == null ? PropertyScope.CATALOG : b.scope;
        this.validator = Optional.ofNullable(b.validator);
        this.enumValues = b.enumValues == null
                ? Optional.empty()
                : Optional.of(Collections.unmodifiableList(new ArrayList<>(b.enumValues)));
    }

    // ----- v1 back-compatible static factories -----

    /** Creates an optional String property with a default value. */
    public static ConnectorPropertyMetadata<String> stringProperty(
            String name, String description, String defaultValue) {
        return ConnectorPropertyMetadata.<String>builder(name, PropertyValueType.STRING)
                .description(description)
                .defaultValue(defaultValue)
                .build();
    }

    /** Creates an optional int property with a default value. */
    public static ConnectorPropertyMetadata<Integer> intProperty(
            String name, String description, int defaultValue) {
        return ConnectorPropertyMetadata.<Integer>builder(name, PropertyValueType.INT)
                .description(description)
                .defaultValue(defaultValue)
                .build();
    }

    /** Creates an optional boolean property with a default value. */
    public static ConnectorPropertyMetadata<Boolean> booleanProperty(
            String name, String description, boolean defaultValue) {
        return ConnectorPropertyMetadata.<Boolean>builder(name, PropertyValueType.BOOLEAN)
                .description(description)
                .defaultValue(defaultValue)
                .build();
    }

    /** Creates a required String property with no default. */
    public static ConnectorPropertyMetadata<String> requiredStringProperty(
            String name, String description) {
        return ConnectorPropertyMetadata.<String>builder(name, PropertyValueType.STRING)
                .description(description)
                .required(true)
                .build();
    }

    // ----- builder entry point -----

    /** Starts a new builder for a property of the given name and value type. */
    public static <T> Builder<T> builder(String name, PropertyValueType valueType) {
        return new Builder<>(name, valueType);
    }

    // ----- getters (v1) -----

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Class<T> getType() {
        return type;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public boolean isRequired() {
        return required;
    }

    // ----- getters (v2) -----

    public PropertyValueType getValueType() {
        return valueType;
    }

    public List<String> getAliases() {
        return aliases;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public boolean isHidden() {
        return hidden;
    }

    public Optional<Deprecation> getDeprecated() {
        return deprecated;
    }

    public boolean isExperimental() {
        return experimental;
    }

    public PropertyScope getScope() {
        return scope;
    }

    public Optional<PropertyValidator<T>> getValidator() {
        return validator;
    }

    public Optional<List<String>> getEnumValues() {
        return enumValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorPropertyMetadata)) {
            return false;
        }
        ConnectorPropertyMetadata<?> that = (ConnectorPropertyMetadata<?>) o;
        return required == that.required
                && sensitive == that.sensitive
                && hidden == that.hidden
                && experimental == that.experimental
                && name.equals(that.name)
                && description.equals(that.description)
                && valueType == that.valueType
                && type.equals(that.type)
                && Objects.equals(defaultValue, that.defaultValue)
                && aliases.equals(that.aliases)
                && deprecated.equals(that.deprecated)
                && scope == that.scope
                && validator.equals(that.validator)
                && enumValues.equals(that.enumValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, valueType, type, defaultValue, required,
                aliases, sensitive, hidden, deprecated, experimental, scope, validator,
                enumValues);
    }

    @Override
    public String toString() {
        return "ConnectorPropertyMetadata{name='" + name
                + "', valueType=" + valueType
                + ", scope=" + scope
                + ", required=" + required
                + ", sensitive=" + sensitive
                + ", hidden=" + hidden
                + ", experimental=" + experimental
                + ", aliases=" + aliases
                + "}";
    }

    /**
     * Marks a property as deprecated; carries optional replacement and version metadata.
     */
    public static final class Deprecation {

        private final String replacement;
        private final String since;
        private final String message;

        private Deprecation(Builder b) {
            this.replacement = b.replacement;
            this.since = b.since == null ? "" : b.since;
            this.message = b.message == null ? "" : b.message;
        }

        /** Replacement property name, or {@code null} if none. */
        public String getReplacement() {
            return replacement;
        }

        /** Version since which the property is deprecated (free-form, may be empty). */
        public String getSince() {
            return since;
        }

        /** Human-readable migration message. */
        public String getMessage() {
            return message;
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Deprecation)) {
                return false;
            }
            Deprecation that = (Deprecation) o;
            return Objects.equals(replacement, that.replacement)
                    && since.equals(that.since)
                    && message.equals(that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(replacement, since, message);
        }

        @Override
        public String toString() {
            return "Deprecation{replacement='" + replacement
                    + "', since='" + since
                    + "', message='" + message + "'}";
        }

        /** Builder for {@link Deprecation}. */
        public static final class Builder {
            private String replacement;
            private String since;
            private String message;

            private Builder() {
            }

            public Builder replacement(String replacement) {
                this.replacement = replacement;
                return this;
            }

            public Builder since(String since) {
                this.since = since;
                return this;
            }

            public Builder message(String message) {
                this.message = message;
                return this;
            }

            public Deprecation build() {
                return new Deprecation(this);
            }
        }
    }

    /**
     * Fluent builder for {@link ConnectorPropertyMetadata}.
     *
     * @param <T> property value type (must match {@link PropertyValueType#javaType()})
     */
    public static final class Builder<T> {

        private final String name;
        private final PropertyValueType valueType;
        private String description;
        private List<String> aliases;
        private T defaultValue;
        private boolean required;
        private boolean sensitive;
        private boolean hidden;
        private Deprecation deprecated;
        private boolean experimental;
        private PropertyScope scope = PropertyScope.CATALOG;
        private PropertyValidator<T> validator;
        private List<String> enumValues;

        private Builder(String name, PropertyValueType valueType) {
            this.name = Objects.requireNonNull(name, "name");
            this.valueType = Objects.requireNonNull(valueType, "valueType");
        }

        public Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        public Builder<T> aliases(List<String> aliases) {
            this.aliases = aliases;
            return this;
        }

        public Builder<T> aliases(String... aliases) {
            this.aliases = aliases == null ? null : new ArrayList<>(Arrays.asList(aliases));
            return this;
        }

        public Builder<T> defaultValue(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder<T> required(boolean required) {
            this.required = required;
            return this;
        }

        public Builder<T> sensitive(boolean sensitive) {
            this.sensitive = sensitive;
            return this;
        }

        public Builder<T> hidden(boolean hidden) {
            this.hidden = hidden;
            return this;
        }

        public Builder<T> deprecated(Deprecation deprecated) {
            this.deprecated = deprecated;
            return this;
        }

        public Builder<T> experimental(boolean experimental) {
            this.experimental = experimental;
            return this;
        }

        public Builder<T> scope(PropertyScope scope) {
            this.scope = scope;
            return this;
        }

        public Builder<T> validator(PropertyValidator<T> validator) {
            this.validator = validator;
            return this;
        }

        public Builder<T> enumValues(List<String> enumValues) {
            this.enumValues = enumValues;
            return this;
        }

        public Builder<T> enumValues(String... enumValues) {
            this.enumValues = enumValues == null
                    ? null
                    : new ArrayList<>(Arrays.asList(enumValues));
            return this;
        }

        public ConnectorPropertyMetadata<T> build() {
            if (defaultValue != null
                    && !valueType.javaType().isAssignableFrom(defaultValue.getClass())) {
                throw new IllegalArgumentException(
                        "defaultValue type " + defaultValue.getClass().getName()
                                + " is not assignable to declared valueType "
                                + valueType + " (" + valueType.javaType().getName() + ")");
            }
            if (valueType == PropertyValueType.ENUM
                    && (enumValues == null || enumValues.isEmpty())) {
                throw new IllegalArgumentException(
                        "ENUM property '" + name + "' requires non-empty enumValues");
            }
            if (aliases != null && aliases.contains(name)) {
                throw new IllegalArgumentException(
                        "aliases for property '" + name + "' must not contain the name itself");
            }
            return new ConnectorPropertyMetadata<>(this);
        }
    }
}
