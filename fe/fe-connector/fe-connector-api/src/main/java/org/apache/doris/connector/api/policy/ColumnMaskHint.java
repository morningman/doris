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

package org.apache.doris.connector.api.policy;

import java.util.Objects;
import java.util.Optional;

/**
 * Plugin-side hint about how a single column should be masked for a given
 * caller. Strictly a hint: the engine retains final authority over policy
 * persistence and resolution. Connectors may return {@link Optional#empty()}
 * from {@link PolicyOps#hintForColumn} when the plugin has no opinion.
 *
 * @param maskKind   Kind of mask the plugin recommends.
 * @param maskExpr   Optional plugin-supplied expression, evaluated by the
 *                   engine if the engine chooses to honour the hint.
 *                   Required for {@link MaskKind#EXPRESSION}.
 */
public record ColumnMaskHint(MaskKind maskKind, Optional<String> maskExpr) {

    public ColumnMaskHint {
        Objects.requireNonNull(maskKind, "maskKind");
        Objects.requireNonNull(maskExpr, "maskExpr");
        if (maskKind == MaskKind.EXPRESSION && maskExpr.isEmpty()) {
            throw new IllegalArgumentException("EXPRESSION maskKind requires a maskExpr");
        }
    }

    /** Convenience factory: full nullification. */
    public static ColumnMaskHint nullify() {
        return new ColumnMaskHint(MaskKind.NULLIFY, Optional.empty());
    }

    /** Convenience factory: redaction (e.g. asterisks). */
    public static ColumnMaskHint redact() {
        return new ColumnMaskHint(MaskKind.REDACT, Optional.empty());
    }

    /** Convenience factory: plugin-supplied expression. */
    public static ColumnMaskHint expression(String expr) {
        return new ColumnMaskHint(MaskKind.EXPRESSION, Optional.of(expr));
    }

    /** Mask strategies the plugin may hint at. */
    public enum MaskKind {
        /** Replace value with NULL. */
        NULLIFY,
        /** Replace value with a redacted constant (engine decides format). */
        REDACT,
        /** Apply the supplied {@code maskExpr} as a SQL expression. */
        EXPRESSION
    }
}
