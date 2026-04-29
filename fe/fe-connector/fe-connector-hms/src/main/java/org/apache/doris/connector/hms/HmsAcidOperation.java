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

package org.apache.doris.connector.hms;

/**
 * Logical operation type for an in-flight Hive ACID write.
 *
 * <p>Maps 1:1 to the corresponding entries in
 * {@code org.apache.hadoop.hive.metastore.api.DataOperationType}.
 * Kept as a plugin-local enum so neither
 * {@link HmsClient} nor {@link HmsWriteOps} expose Hive metastore
 * thrift types in the SPI surface visible to plugin metadata code.</p>
 */
public enum HmsAcidOperation {
    /** Append-only INSERT into an ACID table — staged under {@code delta_*}. */
    INSERT,
    /** UPDATE issued against an ACID table — staged under {@code delete_delta_*} + {@code delta_*}. */
    UPDATE,
    /** DELETE issued against an ACID table — staged under {@code delete_delta_*}. */
    DELETE
}
