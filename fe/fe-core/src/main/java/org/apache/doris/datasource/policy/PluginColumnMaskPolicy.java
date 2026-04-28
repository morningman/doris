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

package org.apache.doris.datasource.policy;

import org.apache.doris.connector.api.policy.ColumnMaskHint;
import org.apache.doris.mysql.privilege.DataMaskPolicy;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Engine-side {@link DataMaskPolicy} adapter for a plugin-supplied
 * {@link ColumnMaskHint}. Used as a fallback when
 * {@code AccessControllerManager.evalDataMaskPolicy} returns empty and a
 * plugin volunteers a hint via {@link PluginDrivenPolicyBridge#hintForColumn}.
 *
 * <p>Strictly engine-internal: never persisted, never advertised to plugins.
 * The {@link #getMaskTypeDef()} expression is rendered from the hint's
 * {@code maskKind} so the existing {@code LogicalCheckPolicy} parser path can
 * consume it without any new SQL surface.</p>
 */
final class PluginColumnMaskPolicy implements DataMaskPolicy {

    private static final String POLICY_IDENT_PREFIX = "plugin-mask-hint:";

    private final String catalog;
    private final String database;
    private final String table;
    private final String column;
    private final ColumnMaskHint hint;

    PluginColumnMaskPolicy(String catalog, String database, String table, String column, ColumnMaskHint hint) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.database = Objects.requireNonNull(database, "database");
        this.table = Objects.requireNonNull(table, "table");
        this.column = Objects.requireNonNull(column, "column");
        this.hint = Objects.requireNonNull(hint, "hint");
    }

    ColumnMaskHint getHint() {
        return hint;
    }

    @Override
    public String getMaskTypeDef() {
        switch (hint.maskKind()) {
            case NULLIFY:
                return "NULL";
            case REDACT:
                // Engine renders REDACT as a constant string; the column type
                // coercion is handled by the surrounding alias / cast pipeline
                // in LogicalCheckPolicy.
                return "'***'";
            case EXPRESSION:
                Preconditions.checkArgument(hint.maskExpr().isPresent(),
                        "EXPRESSION hint without maskExpr");
                return hint.maskExpr().get();
            default:
                throw new IllegalStateException("Unknown MaskKind: " + hint.maskKind());
        }
    }

    @Override
    public String getPolicyIdent() {
        return POLICY_IDENT_PREFIX + catalog + "." + database + "." + table + "." + column
                + ":" + hint.maskKind();
    }

    @Override
    public String toString() {
        return "PluginColumnMaskPolicy{"
                + "catalog='" + catalog + '\''
                + ", database='" + database + '\''
                + ", table='" + table + '\''
                + ", column='" + column + '\''
                + ", hint=" + hint
                + '}';
    }
}
