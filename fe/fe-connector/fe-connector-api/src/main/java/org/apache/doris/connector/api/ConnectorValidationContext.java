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

/**
 * Context provided to connectors during pre-creation validation (CREATE CATALOG).
 *
 * <p>The engine implements this interface to expose infrastructure services
 * (driver validation, checksum computation, BE connectivity testing) that
 * connectors may need during validation. Each connector type calls only the
 * services relevant to its own validation logic.</p>
 *
 * <p>This keeps connector-specific validation inside the connector while
 * the engine provides the underlying capabilities.</p>
 */
public interface ConnectorValidationContext {

    /** Returns the catalog ID. */
    long getCatalogId();

    /** Returns a catalog property value, or {@code null} if not set. */
    String getProperty(String key);

    /** Stores a computed property back into the catalog configuration. */
    void storeProperty(String key, String value);

    /**
     * Validates a driver URL: format, whitelist, secure_path, file existence.
     * Returns the resolved full driver URL.
     *
     * @param driverUrl the raw driver URL from catalog properties
     * @return the resolved, validated full URL
     * @throws Exception if the driver URL is invalid or inaccessible
     */
    String validateAndResolveDriverPath(String driverUrl) throws Exception;

    /**
     * Computes the MD5 checksum for a driver file at the given URL.
     *
     * @param driverUrl the driver URL to checksum
     * @return the hex-encoded MD5 checksum
     * @throws Exception if checksum computation fails
     */
    String computeDriverChecksum(String driverUrl) throws Exception;

    /**
     * Tests BE→external JDBC connectivity via BRPC.
     *
     * <p>The connector builds the serialized Thrift payload (TTableDescriptor);
     * the engine handles finding an alive backend and BRPC transport.</p>
     *
     * @param serializedTableDescriptor TSerializer-serialized TTableDescriptor
     * @param tableTypeValue TOdbcTableType enum value
     * @param testQuery SQL query to execute (e.g., "SELECT 1")
     * @throws Exception if the BE connectivity test fails
     */
    void testBeJdbcConnection(byte[] serializedTableDescriptor, int tableTypeValue,
            String testQuery) throws Exception;
}
