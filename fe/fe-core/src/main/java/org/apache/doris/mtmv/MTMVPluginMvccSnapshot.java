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

package org.apache.doris.mtmv;

import com.google.gson.annotations.SerializedName;

import java.util.Objects;

/**
 * MTMV refresh snapshot for plugin-driven external tables that hold an
 * opaque, codec-encoded {@code ConnectorMvccSnapshot} payload.
 *
 * <p>The payload is the Base64 form produced by
 * {@code MvccSnapshotPersistence.serialize(...)} and is meaningful only to
 * the same connector type ({@link #getConnectorType()}). The
 * {@code PluginMtmvSnapshotBridge} round-trips between this type and the
 * SPI-side {@code ConnectorMvccSnapshot} using the connector's codec.</p>
 *
 * <p>{@link #getSnapshotVersion()} returns a stable hash of the payload so
 * legacy comparisons against {@code MTMVSnapshotIf#getSnapshotVersion()}
 * still distinguish two distinct snapshots; equality, however, is based on
 * the (connectorType, payload) pair so two snapshots with colliding hashes
 * remain non-equal.</p>
 */
public class MTMVPluginMvccSnapshot implements MTMVSnapshotIf {

    @SerializedName("ct")
    private final String connectorType;

    @SerializedName("p")
    private final String base64Payload;

    public MTMVPluginMvccSnapshot(String connectorType, String base64Payload) {
        this.connectorType = Objects.requireNonNull(connectorType, "connectorType");
        this.base64Payload = Objects.requireNonNull(base64Payload, "base64Payload");
    }

    public String getConnectorType() {
        return connectorType;
    }

    public String getBase64Payload() {
        return base64Payload;
    }

    @Override
    public long getSnapshotVersion() {
        // Stable, non-cryptographic mix of payload + type used only as a
        // monotonic-ish discriminator for legacy MTMV staleness checks that
        // call getSnapshotVersion(). Real equality goes through equals().
        return ((long) connectorType.hashCode() << 32) ^ (base64Payload.hashCode() & 0xffffffffL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MTMVPluginMvccSnapshot that = (MTMVPluginMvccSnapshot) o;
        return connectorType.equals(that.connectorType) && base64Payload.equals(that.base64Payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorType, base64Payload);
    }

    @Override
    public String toString() {
        return "MTMVPluginMvccSnapshot{"
                + "connectorType='" + connectorType + '\''
                + ", base64Payload='" + base64Payload + '\''
                + '}';
    }
}
