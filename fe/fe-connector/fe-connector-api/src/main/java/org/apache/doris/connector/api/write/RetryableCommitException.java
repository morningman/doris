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

package org.apache.doris.connector.api.write;

/**
 * Thrown by a plugin commit path to signal that the commit failed in a way
 * that the engine may safely retry (e.g., optimistic-lock conflict,
 * transient metastore 5xx). Carries an optional suggested back-off (D1).
 */
public final class RetryableCommitException extends RuntimeException {
    private final java.time.Duration suggestedBackoff;

    public RetryableCommitException(String msg) {
        this(msg, null, java.time.Duration.ZERO);
    }

    public RetryableCommitException(String msg, Throwable cause) {
        this(msg, cause, java.time.Duration.ZERO);
    }

    public RetryableCommitException(String msg, Throwable cause, java.time.Duration suggestedBackoff) {
        super(msg, cause);
        this.suggestedBackoff = java.util.Objects.requireNonNull(suggestedBackoff, "suggestedBackoff");
    }

    /** Returns the plugin-suggested retry back-off (never null; defaults to ZERO). */
    public java.time.Duration suggestedBackoff() {
        return suggestedBackoff;
    }
}
