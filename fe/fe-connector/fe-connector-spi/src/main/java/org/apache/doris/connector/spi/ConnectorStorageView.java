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

package org.apache.doris.connector.spi;

import java.util.Map;

/**
 * The resolved storage view of one scan or write: every "how is this storage accessed" question a
 * connector hands to BE — credentials, canonical URIs, the BE reader family — answered from ONE
 * token→storage-config derivation ({@link ConnectorStorageContext#resolveStorage}).
 *
 * <p>The derivation (storage binding + config build) is a pure function of the scan-invariant
 * per-table vended token, but a scan applies these answers O(N_files + N_deletes) times, so the
 * view hoists it out of the per-file loop. The engine derives lazily on first use, preserving the
 * exception timing of the per-call methods this view replaces.
 *
 * <p>A view is used single-threaded within one scan/write (the streaming split pump drives one
 * thread), so implementations need no lock. Build one view where the vended token is extracted and
 * thread it through the per-file builders; do not share a view across scans.
 */
public interface ConnectorStorageView {

    /**
     * The per-table vended credentials normalized to the BE-facing storage property map
     * ({@code AWS_ACCESS_KEY} / {@code AWS_ENDPOINT} / {@code fs.azure.*} ...). Empty when the
     * view was resolved without a vended token, or on any normalization error (fail-soft: a
     * malformed token degrades gracefully rather than killing the scan) — the connector overlays
     * this on its static credentials, so empty means "no overlay".
     */
    Map<String, String> backendCredentials();

    /**
     * Normalizes a raw storage URI (a data file, delete file, or write output path) to BE's
     * canonical, scheme-dispatched form using this view's storage config (vended when the token
     * yielded one, the catalog's static map otherwise). Null/blank is returned unchanged; a path
     * that cannot be normalized fails loud (it would otherwise silently corrupt reads).
     */
    String normalizeUri(String rawUri);

    /**
     * Resolves the BE reader family for a raw storage URI as a {@code TFileType} enum name (the
     * SPI stays Thrift-free; the connector maps it back). Cheap per path — the storage config is
     * already derived — so a connector stamps every range/sink with its own path's answer instead
     * of assuming one family for the whole scan. The decision is the engine's (the bound storage
     * answers for itself); the connector only forwards it, e.g. via
     * {@code ConnectorScanRange.getBackendFileType()}.
     */
    String backendFileType(String rawUri);
}
