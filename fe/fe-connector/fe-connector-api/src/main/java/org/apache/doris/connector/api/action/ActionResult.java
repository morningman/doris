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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable action result. Rows are defensively deep-copied so caller
 * mutations do not leak into the returned value.
 */
public final class ActionResult {

    /** Lifecycle state of an action result. */
    public enum Lifecycle { COMPLETED, STARTED_ASYNC, FAILED }

    private final List<Map<String, Object>> rows;
    private final Lifecycle lifecycle;
    private final String handleId;
    private final String errorMessage;

    private ActionResult(List<Map<String, Object>> rows, Lifecycle lifecycle,
                         String handleId, String errorMessage) {
        Objects.requireNonNull(rows, "rows");
        Objects.requireNonNull(lifecycle, "lifecycle");
        List<Map<String, Object>> copied = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Objects.requireNonNull(row, "row");
            copied.add(Collections.unmodifiableMap(new LinkedHashMap<>(row)));
        }
        this.rows = Collections.unmodifiableList(copied);
        this.lifecycle = lifecycle;
        this.handleId = handleId;
        this.errorMessage = errorMessage;
    }

    public List<Map<String, Object>> rows() {
        return rows;
    }

    public Lifecycle lifecycle() {
        return lifecycle;
    }

    public Optional<String> handleId() {
        return Optional.ofNullable(handleId);
    }

    public Optional<String> errorMessage() {
        return Optional.ofNullable(errorMessage);
    }

    /** Completed result with the given rows. */
    public static ActionResult completed(List<Map<String, Object>> rows) {
        return new ActionResult(rows, Lifecycle.COMPLETED, null, null);
    }

    /** Completed side-effect-only result with no rows. */
    public static ActionResult completedEmpty() {
        return new ActionResult(Collections.emptyList(), Lifecycle.COMPLETED, null, null);
    }

    /** Async result; engine polls on {@code handleId}. */
    public static ActionResult startedAsync(String handleId) {
        Objects.requireNonNull(handleId, "handleId");
        if (handleId.isBlank()) {
            throw new IllegalArgumentException("handleId must not be blank");
        }
        return new ActionResult(Collections.emptyList(), Lifecycle.STARTED_ASYNC, handleId, null);
    }

    /** Failed result carrying an error message. */
    public static ActionResult failed(String errorMessage) {
        Objects.requireNonNull(errorMessage, "errorMessage");
        return new ActionResult(Collections.emptyList(), Lifecycle.FAILED, null, errorMessage);
    }

    @Override
    public String toString() {
        return "ActionResult{lifecycle=" + lifecycle
                + ", rows=" + rows
                + ", handleId=" + Optional.ofNullable(handleId)
                + ", errorMessage=" + Optional.ofNullable(errorMessage) + "}";
    }
}
