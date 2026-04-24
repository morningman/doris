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

package org.apache.doris.connector.iceberg.api;

/**
 * ServiceLoader-discovered factory for {@link IcebergBackend} implementations.
 *
 * <p>Each {@code fe-connector-iceberg-backend-<name>} jar registers exactly
 * one factory in {@code META-INF/services/org.apache.doris.connector.iceberg.api.IcebergBackendFactory}.
 * The orchestrator scans the plugin classloader for all factories on first use
 * and matches by {@link #name()} against the value of
 * {@code iceberg.catalog.type}.
 *
 * <p>Implementations must have a public no-arg constructor.
 */
public interface IcebergBackendFactory {

    /**
     * Canonical lowercase backend identifier (e.g. {@code "hms"},
     * {@code "rest"}, {@code "glue"}, {@code "dlf"}, {@code "s3tables"},
     * {@code "hadoop"}). Must equal the {@link IcebergBackend#name()} of the
     * backend produced by {@link #create()}.
     */
    String name();

    /**
     * Create a fresh {@link IcebergBackend} instance. Backends are stateless
     * with respect to a single {@link IcebergBackendContext}; the orchestrator
     * may cache the instance per Doris catalog.
     */
    IcebergBackend create();
}
