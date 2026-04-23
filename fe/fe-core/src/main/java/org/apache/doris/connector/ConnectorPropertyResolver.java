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

package org.apache.doris.connector;

import org.apache.doris.connector.api.ConnectorPropertyException;
import org.apache.doris.connector.api.ConnectorPropertyMetadata;
import org.apache.doris.connector.api.PropertyValidator;
import org.apache.doris.connector.api.PropertyValueType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Engine-side resolver that converts a raw {@code Map<String,String>} of user-supplied
 * property entries into a typed, validated {@code Map<String,Object>} according to a
 * list of {@link ConnectorPropertyMetadata} descriptors.
 *
 * <p>Per design doc §3 (D12), this class enforces (in order): alias collapse, required
 * check, per-type conversion, validator invocation, default fill-in, unknown-key
 * rejection, and deprecation warning.</p>
 *
 * <p>All errors surface as {@link ConnectorPropertyException} carrying a stable code:
 * {@code AMBIGUOUS_ALIAS}, {@code REQUIRED_MISSING}, {@code TYPE_MISMATCH},
 * {@code UNKNOWN_PROPERTY}.</p>
 */
public final class ConnectorPropertyResolver {

    private static final Logger LOG = LogManager.getLogger(ConnectorPropertyResolver.class);

    private static final Pattern DURATION_SHORTHAND =
            Pattern.compile("^\\s*(\\d+)\\s*(ms|s|m|h|d)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SIZE_PATTERN =
            Pattern.compile("^\\s*(\\d+)\\s*([kmgt]?)b?\\s*$", Pattern.CASE_INSENSITIVE);

    private ConnectorPropertyResolver() {
    }

    /** Resolve {@code rawProps} against the given descriptor list. */
    public static Map<String, Object> resolve(
            List<ConnectorPropertyMetadata<?>> descriptors,
            Map<String, String> rawProps) {
        Resolution r = doResolve(descriptors, rawProps);
        return r.values;
    }

    /**
     * Same as {@link #resolve} but additionally returns a sanitized view of the raw
     * user input (sensitive values replaced with {@code "***"}). Keys absent from the
     * raw input are not present in the result.
     */
    public static Map<String, String> sanitize(
            List<ConnectorPropertyMetadata<?>> descriptors,
            Map<String, String> rawProps) {
        Resolution r = doResolve(descriptors, rawProps);
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : r.canonicalRaw.entrySet()) {
            ConnectorPropertyMetadata<?> md = r.byName.get(e.getKey());
            if (md != null && md.isSensitive()) {
                out.put(e.getKey(), "***");
            } else {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    // ----------------------------------------------------------------------
    // internals
    // ----------------------------------------------------------------------

    private static Resolution doResolve(
            List<ConnectorPropertyMetadata<?>> descriptors,
            Map<String, String> rawProps) {
        if (descriptors == null) {
            descriptors = Collections.emptyList();
        }
        if (rawProps == null) {
            rawProps = Collections.emptyMap();
        }

        Map<String, ConnectorPropertyMetadata<?>> byName = new HashMap<>();
        Map<String, ConnectorPropertyMetadata<?>> byKey = new HashMap<>();
        for (ConnectorPropertyMetadata<?> md : descriptors) {
            byName.put(md.getName(), md);
            byKey.put(md.getName(), md);
            for (String alias : md.getAliases()) {
                byKey.put(alias, md);
            }
        }

        Set<String> knownKeys = byKey.keySet();
        for (String userKey : rawProps.keySet()) {
            if (!knownKeys.contains(userKey)) {
                throw new ConnectorPropertyException(
                        "UNKNOWN_PROPERTY", "unknown property '" + userKey + "'");
            }
        }

        Map<String, String> canonicalRaw = new LinkedHashMap<>();
        for (ConnectorPropertyMetadata<?> md : descriptors) {
            String name = md.getName();
            boolean hasCanonical = rawProps.containsKey(name);
            String chosenAlias = null;
            for (String alias : md.getAliases()) {
                if (rawProps.containsKey(alias)) {
                    if (hasCanonical || chosenAlias != null) {
                        String other = hasCanonical ? name : chosenAlias;
                        throw new ConnectorPropertyException(
                                "AMBIGUOUS_ALIAS",
                                "property '" + name + "' supplied via both '"
                                        + other + "' and alias '" + alias + "'");
                    }
                    chosenAlias = alias;
                }
            }
            if (hasCanonical) {
                canonicalRaw.put(name, rawProps.get(name));
            } else if (chosenAlias != null) {
                canonicalRaw.put(name, rawProps.get(chosenAlias));
            }
        }

        Map<String, Object> resolved = new LinkedHashMap<>();
        ResolverValidationContext ctx = new ResolverValidationContext(resolved.keySet(), canonicalRaw);

        for (ConnectorPropertyMetadata<?> md : descriptors) {
            String name = md.getName();
            if (!canonicalRaw.containsKey(name)) {
                if (md.isRequired()) {
                    throw new ConnectorPropertyException(
                            "REQUIRED_MISSING", "property '" + name + "' is required");
                }
                resolved.put(name, md.getDefaultValue());
                continue;
            }

            String raw = canonicalRaw.get(name);
            Object converted = convert(md, raw);
            invokeValidator(md, converted, ctx);
            resolved.put(name, converted);

            Optional<ConnectorPropertyMetadata.Deprecation> dep = md.getDeprecated();
            if (dep.isPresent()) {
                ConnectorPropertyMetadata.Deprecation d = dep.get();
                String replacement = d.getReplacement() == null ? "" : d.getReplacement();
                LOG.warn("property '{}' is deprecated since {}; use '{}' instead — {}",
                        name, d.getSince(), replacement, d.getMessage());
            }
        }

        return new Resolution(resolved, canonicalRaw, byName);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void invokeValidator(
            ConnectorPropertyMetadata<?> md,
            Object converted,
            PropertyValidator.ValidationContext ctx) {
        Optional<? extends PropertyValidator<?>> opt = md.getValidator();
        if (!opt.isPresent()) {
            return;
        }
        PropertyValidator validator = opt.get();
        validator.validate(md.getName(), converted, ctx);
    }

    private static Object convert(ConnectorPropertyMetadata<?> md, String raw) {
        PropertyValueType type = md.getValueType();
        String trimmed = raw == null ? "" : raw.trim();
        try {
            switch (type) {
                case STRING:
                    return raw;
                case INT:
                    return Integer.parseInt(trimmed);
                case LONG:
                    return Long.parseLong(trimmed);
                case BOOLEAN:
                    return parseBoolean(md.getName(), raw, trimmed);
                case DURATION:
                    return parseDuration(md.getName(), raw, trimmed);
                case SIZE:
                    return parseSize(md.getName(), raw, trimmed);
                case ENUM:
                    return parseEnum(md, raw);
                case STRING_LIST:
                    return parseStringList(raw);
                case MAP:
                    return parseMap(md.getName(), raw);
                default:
                    throw typeMismatch(md.getName(), type, raw);
            }
        } catch (ConnectorPropertyException e) {
            throw e;
        } catch (RuntimeException e) {
            throw typeMismatch(md.getName(), type, raw);
        }
    }

    private static Boolean parseBoolean(String name, String raw, String trimmed) {
        if ("true".equalsIgnoreCase(trimmed)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(trimmed)) {
            return Boolean.FALSE;
        }
        throw typeMismatch(name, PropertyValueType.BOOLEAN, raw);
    }

    private static Duration parseDuration(String name, String raw, String trimmed) {
        Matcher m = DURATION_SHORTHAND.matcher(trimmed);
        if (m.matches()) {
            long n = Long.parseLong(m.group(1));
            String unit = m.group(2).toLowerCase(Locale.ROOT);
            switch (unit) {
                case "ms":
                    return Duration.ofMillis(n);
                case "s":
                    return Duration.ofSeconds(n);
                case "m":
                    return Duration.ofMinutes(n);
                case "h":
                    return Duration.ofHours(n);
                case "d":
                    return Duration.ofDays(n);
                default:
                    throw typeMismatch(name, PropertyValueType.DURATION, raw);
            }
        }
        try {
            return Duration.parse(trimmed);
        } catch (DateTimeParseException e) {
            throw typeMismatch(name, PropertyValueType.DURATION, raw);
        }
    }

    private static Long parseSize(String name, String raw, String trimmed) {
        Matcher m = SIZE_PATTERN.matcher(trimmed);
        if (!m.matches()) {
            throw typeMismatch(name, PropertyValueType.SIZE, raw);
        }
        long n = Long.parseLong(m.group(1));
        String suffix = m.group(2).toLowerCase(Locale.ROOT);
        long mult;
        switch (suffix) {
            case "":
                mult = 1L;
                break;
            case "k":
                mult = 1024L;
                break;
            case "m":
                mult = 1024L * 1024L;
                break;
            case "g":
                mult = 1024L * 1024L * 1024L;
                break;
            case "t":
                mult = 1024L * 1024L * 1024L * 1024L;
                break;
            default:
                throw typeMismatch(name, PropertyValueType.SIZE, raw);
        }
        return n * mult;
    }

    private static String parseEnum(ConnectorPropertyMetadata<?> md, String raw) {
        Optional<List<String>> values = md.getEnumValues();
        if (!values.isPresent() || !values.get().contains(raw)) {
            throw typeMismatch(md.getName(), PropertyValueType.ENUM, raw);
        }
        return raw;
    }

    private static List<String> parseStringList(String raw) {
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }
        java.util.ArrayList<String> out = new java.util.ArrayList<>();
        for (String part : raw.split(",")) {
            String t = part.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    private static Map<String, String> parseMap(String name, String raw) {
        Map<String, String> out = new LinkedHashMap<>();
        if (raw == null || raw.trim().isEmpty()) {
            return out;
        }
        for (String pair : raw.split(",")) {
            String p = pair.trim();
            if (p.isEmpty()) {
                continue;
            }
            int eq = p.indexOf('=');
            if (eq <= 0 || eq == p.length() - 1) {
                throw typeMismatch(name, PropertyValueType.MAP, raw);
            }
            out.put(p.substring(0, eq).trim(), p.substring(eq + 1).trim());
        }
        return out;
    }

    private static ConnectorPropertyException typeMismatch(
            String name, PropertyValueType type, String raw) {
        return new ConnectorPropertyException(
                "TYPE_MISMATCH",
                "property '" + name + "' expected " + type + ", got '" + raw + "'");
    }

    /** Internal carrier of a single resolution pass. */
    private static final class Resolution {
        final Map<String, Object> values;
        final Map<String, String> canonicalRaw;
        final Map<String, ConnectorPropertyMetadata<?>> byName;

        Resolution(Map<String, Object> values,
                   Map<String, String> canonicalRaw,
                   Map<String, ConnectorPropertyMetadata<?>> byName) {
            this.values = values;
            this.canonicalRaw = canonicalRaw;
            this.byName = byName;
        }
    }

    private static final class ResolverValidationContext implements PropertyValidator.ValidationContext {
        private final Set<String> resolvedKeys;
        private final Map<String, String> canonicalRaw;

        ResolverValidationContext(Set<String> resolvedKeys, Map<String, String> canonicalRaw) {
            this.resolvedKeys = resolvedKeys;
            this.canonicalRaw = canonicalRaw;
        }

        @Override
        public Set<String> allKeys() {
            return new HashSet<>(resolvedKeys);
        }

        @Override
        public String rawValue(String key) {
            return canonicalRaw.get(key);
        }
    }
}
