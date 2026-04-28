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

package org.apache.doris.connector.iceberg.source;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.TimestampType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Plugin-private port of the legacy {@code IcebergUtils.getPartitionDataJson}
 * and {@code IcebergUtils.getPartitionInfoMap} helpers. Pure Java, no
 * fe-core dependency. Used by {@link IcebergScanPlanProvider} to populate
 * the {@code partitionDataJson} and {@code partitionValues} fields of an
 * {@link IcebergScanRange}.
 *
 * <p>Serialization rules mirror legacy
 * {@code IcebergUtils#serializePartitionValue} byte-for-byte for the
 * primitive iceberg partition types (boolean / int / long / float / double /
 * string / uuid / decimal / date / time / timestamp). BINARY / FIXED return
 * {@code null} from {@link #buildIdentityPartitionValues} so the caller skips
 * dynamic partition pruning, matching legacy behaviour.</p>
 */
final class IcebergPartitionDataUtil {

    private IcebergPartitionDataUtil() {
    }

    /**
     * Serialize the partition tuple to a JSON array of strings, matching
     * {@code IcebergUtils.getPartitionDataJson}. Used as the wire form of
     * {@code TIcebergFileDesc#partitionDataJson}.
     *
     * @param partitionData the iceberg {@link StructLike} partition tuple
     *                      from {@code FileScanTask#file().partition()}
     * @param partitionSpec the partition spec resolved by spec id
     * @param timeZone      the session time zone (used for timestamptz)
     * @return JSON string of the form {@code ["v1","v2",null,...]}
     */
    static String getPartitionDataJson(StructLike partitionData,
                                       PartitionSpec partitionSpec,
                                       String timeZone) {
        List<NestedField> fields = partitionSpec.partitionType().fields();
        if (fields.size() != partitionSpec.fields().size()) {
            throw new IllegalStateException("PartitionData fields size does not match PartitionSpec fields size");
        }
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            NestedField field = fields.get(i);
            Object value = partitionData.get(i, Object.class);
            String s;
            try {
                s = serializePartitionValue(field.type(), value, timeZone);
            } catch (UnsupportedOperationException ignored) {
                s = null;
            }
            appendJsonString(sb, s);
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * Build the identity-only partition value map. Returns {@code null} if
     * any partition field uses a non-identity transform or a binary/fixed
     * type, matching legacy {@code getPartitionInfoMap} semantics.
     */
    static Map<String, String> buildIdentityPartitionValues(StructLike partitionData,
                                                            PartitionSpec partitionSpec,
                                                            String timeZone) {
        List<NestedField> fields = partitionSpec.partitionType().fields();
        List<PartitionField> partitionFields = partitionSpec.fields();
        if (fields.size() != partitionFields.size()) {
            throw new IllegalStateException("PartitionData fields size does not match PartitionSpec fields size");
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            NestedField field = fields.get(i);
            PartitionField partitionField = partitionFields.get(i);
            if (!partitionField.transform().isIdentity()) {
                return null;
            }
            TypeID typeId = field.type().typeId();
            if (typeId == TypeID.BINARY || typeId == TypeID.FIXED) {
                return null;
            }
            Object value = partitionData.get(i, Object.class);
            String s;
            try {
                s = serializePartitionValue(field.type(), value, timeZone);
            } catch (UnsupportedOperationException ignored) {
                return null;
            }
            out.put(field.name(), s);
        }
        return out;
    }

    private static String serializePartitionValue(Type type, Object value, String timeZone) {
        switch (type.typeId()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case DECIMAL:
                return value == null ? null : value.toString();
            case DATE:
                if (value == null) {
                    return null;
                }
                return LocalDate.ofEpochDay(((Number) value).intValue())
                        .format(DateTimeFormatter.ISO_LOCAL_DATE);
            case TIME:
                if (value == null) {
                    return null;
                }
                long micros = ((Number) value).longValue();
                return LocalTime.ofNanoOfDay(micros * 1000)
                        .format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP:
                if (value == null) {
                    return null;
                }
                long ts = ((Number) value).longValue();
                LocalDateTime ldt = LocalDateTime.ofEpochSecond(
                        Math.floorDiv(ts, 1_000_000L),
                        (int) (Math.floorMod(ts, 1_000_000L) * 1000L),
                        ZoneOffset.UTC);
                if (((TimestampType) type).shouldAdjustToUTC()) {
                    ldt = ldt.atZone(ZoneId.of("UTC"))
                            .withZoneSameInstant(ZoneId.of(timeZone))
                            .toLocalDateTime();
                }
                return ldt.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            default:
                throw new UnsupportedOperationException("Unsupported type for serializePartitionValue: " + type);
        }
    }

    private static void appendJsonString(StringBuilder sb, String s) {
        if (s == null) {
            sb.append("null");
            return;
        }
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        sb.append('"');
    }
}
