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

package org.apache.doris.connector.timetravel;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.connector.api.timetravel.ConnectorRefSpec;
import org.apache.doris.connector.api.timetravel.RefKind;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Bridges the legacy Nereids {@link TableScanParams} AST node (produced by
 * the {@code @branch(name=...)} / {@code @tag(name=...)} table-valued
 * function syntax) into the D5 SPI sealed type {@link ConnectorRefSpec}.
 *
 * <p>Sibling of {@link ConnectorTableVersionResolver}: that resolver bridges
 * {@code FOR VERSION/TIME AS OF}, this one bridges named refs. The output
 * is fed to the time-travel-aware
 * {@link org.apache.doris.connector.api.ConnectorTableOps#getTableHandle
 * (org.apache.doris.connector.api.ConnectorSession, String, String,
 * java.util.Optional, java.util.Optional)} overload and to the
 * {@link org.apache.doris.connector.api.scan.ConnectorScanRequest}
 * propagated into {@code planScan}.</p>
 *
 * <p>The legacy {@code TableScanParams} carries the ref name in either of
 * two equivalent shapes (see grammar):</p>
 * <ul>
 *     <li>map form: {@code @branch('name'='b1')} → {@code mapParams.get("name")}</li>
 *     <li>list form: {@code @branch('b1')} → {@code listParams.get(0)}</li>
 * </ul>
 * Both shapes resolve to the same {@link ConnectorRefSpec}. Incremental
 * read params ({@link TableScanParams#incrementalRead()}) and {@code null}
 * inputs return {@link Optional#empty()}.
 */
public final class ConnectorRefSpecResolver {

    private ConnectorRefSpecResolver() {
    }

    /**
     * Resolve a legacy {@link TableScanParams} into a {@link ConnectorRefSpec}.
     *
     * @param params legacy AST scan params, may be {@code null}
     * @return the corresponding {@link ConnectorRefSpec}, or
     *         {@link Optional#empty()} for {@code null} input or for
     *         param types other than {@code BRANCH}/{@code TAG}
     * @throws IllegalArgumentException if the ref name is missing or blank
     */
    public static Optional<ConnectorRefSpec> resolve(TableScanParams params) {
        if (params == null) {
            return Optional.empty();
        }
        RefKind kind;
        if (params.isBranch()) {
            kind = RefKind.BRANCH;
        } else if (params.isTag()) {
            kind = RefKind.TAG;
        } else {
            return Optional.empty();
        }
        String name = extractName(params, kind);
        return Optional.of(ConnectorRefSpec.builder().name(name).kind(kind).build());
    }

    private static String extractName(TableScanParams params, RefKind kind) {
        Map<String, String> mapParams = params.getMapParams();
        String name = mapParams != null ? mapParams.get(TableScanParams.PARAMS_NAME) : null;
        if (name == null || name.isEmpty()) {
            List<String> listParams = params.getListParams();
            if (listParams != null && !listParams.isEmpty()) {
                name = listParams.get(0);
            }
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                    kind.name().toLowerCase() + " ref name is required (use 'name'=... or positional arg)");
        }
        return name;
    }
}
