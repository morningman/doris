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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.timetravel.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.timetravel.ConnectorTableVersion;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Iceberg-side {@link ConnectorMvccSnapshot}: a (snapshotId, commitTime)
 * pair pinning a query to a specific iceberg snapshot.
 *
 * <p>Round-trips across FE→BE and to MTMV refresh metadata via the
 * {@link Codec} nested type; the wire format is a fixed-size 24-byte
 * binary frame:
 * <pre>
 *   int32   magic       = 0x49434542  ("ICEB" big-endian)
 *   int32   formatVer   = 1
 *   int64   snapshotId
 *   int64   commitTimeMs (epoch millis)
 * </pre>
 * Decoding rejects any other magic / format version with a
 * {@link DorisConnectorException}. (D5 §3.3 / D5.5)
 */
public final class IcebergConnectorMvccSnapshot implements ConnectorMvccSnapshot {

    static final int MAGIC = 0x49434542; // "ICEB"
    static final int FORMAT_VERSION = 1;
    static final int FRAME_BYTES = 4 + 4 + 8 + 8;

    private final long snapshotId;
    private final Instant commitTime;

    public IcebergConnectorMvccSnapshot(long snapshotId, Instant commitTime) {
        this.snapshotId = snapshotId;
        this.commitTime = Objects.requireNonNull(commitTime, "commitTime");
    }

    public long snapshotId() {
        return snapshotId;
    }

    @Override
    public Instant commitTime() {
        return commitTime;
    }

    @Override
    public Optional<ConnectorTableVersion> asVersion() {
        return Optional.of(new ConnectorTableVersion.BySnapshotId(snapshotId));
    }

    @Override
    public String toOpaqueToken() {
        return "iceberg:" + snapshotId + ":" + commitTime.toEpochMilli();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IcebergConnectorMvccSnapshot)) {
            return false;
        }
        IcebergConnectorMvccSnapshot that = (IcebergConnectorMvccSnapshot) o;
        return snapshotId == that.snapshotId && commitTime.equals(that.commitTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, commitTime);
    }

    @Override
    public String toString() {
        return "IcebergConnectorMvccSnapshot{snapshotId=" + snapshotId
                + ", commitTime=" + commitTime + '}';
    }

    /** Plug-in-private codec registered through
     *  {@link IcebergConnectorProvider#getMvccSnapshotCodec()}. */
    public static final class Codec implements ConnectorMvccSnapshot.Codec {

        @Override
        public byte[] encode(ConnectorMvccSnapshot s) {
            Objects.requireNonNull(s, "snapshot");
            if (!(s instanceof IcebergConnectorMvccSnapshot)) {
                throw new DorisConnectorException(
                        "IcebergConnectorMvccSnapshot.Codec cannot encode: " + s.getClass().getName());
            }
            IcebergConnectorMvccSnapshot is = (IcebergConnectorMvccSnapshot) s;
            ByteArrayOutputStream baos = new ByteArrayOutputStream(FRAME_BYTES);
            try (DataOutputStream out = new DataOutputStream(baos)) {
                out.writeInt(MAGIC);
                out.writeInt(FORMAT_VERSION);
                out.writeLong(is.snapshotId);
                out.writeLong(is.commitTime.toEpochMilli());
            } catch (IOException e) {
                throw new DorisConnectorException("Failed to encode IcebergConnectorMvccSnapshot", e);
            }
            return baos.toByteArray();
        }

        @Override
        public ConnectorMvccSnapshot decode(byte[] bytes) {
            Objects.requireNonNull(bytes, "bytes");
            if (bytes.length != FRAME_BYTES) {
                throw new DorisConnectorException(
                        "Iceberg mvcc snapshot frame length must be " + FRAME_BYTES
                                + " bytes, got " + bytes.length);
            }
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
                int magic = in.readInt();
                if (magic != MAGIC) {
                    throw new DorisConnectorException(
                            "Iceberg mvcc snapshot bad magic: 0x"
                                    + Integer.toHexString(magic));
                }
                int formatVer = in.readInt();
                if (formatVer != FORMAT_VERSION) {
                    throw new DorisConnectorException(
                            "Iceberg mvcc snapshot unsupported format version: " + formatVer);
                }
                long snapshotId = in.readLong();
                long commitMs = in.readLong();
                return new IcebergConnectorMvccSnapshot(snapshotId, Instant.ofEpochMilli(commitMs));
            } catch (IOException e) {
                throw new DorisConnectorException("Failed to decode IcebergConnectorMvccSnapshot", e);
            }
        }
    }
}
