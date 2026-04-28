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

package org.apache.doris.datasource;

import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Covers {@link PluginDrivenScanNode#applyCountToRanges}, the per-range
 * count-distribution helper invoked when a connector reports a
 * metadata-only {@code COUNT(*)} via
 * {@link org.apache.doris.connector.api.scan.ConnectorScanPlanProvider#getCountPushdownResult}.
 *
 * <p>The algorithm mirrors legacy
 * {@code IcebergScanNode#assignCountToSplits}: each of the first
 * {@code N-1} ranges receives {@code total/N}, the LAST receives
 * {@code total/N + total%N}.</p>
 */
public class PluginDrivenScanNodeCountPushdownTest {

    /**
     * Test double that records the last count fed in via
     * {@link #withTableLevelRowCount}, allowing us to assert the
     * distribution without depending on the iceberg plugin module
     * (which fe-core does not have on its classpath).
     */
    private static final class CountTrackingRange implements ConnectorScanRange {
        private static final long serialVersionUID = 1L;
        private final long count;
        private final String id;

        CountTrackingRange(String id, long count) {
            this.id = id;
            this.count = count;
        }

        long getCount() {
            return count;
        }

        String getId() {
            return id;
        }

        @Override
        public ConnectorScanRangeType getRangeType() {
            return ConnectorScanRangeType.FILE_SCAN;
        }

        @Override
        public Map<String, String> getProperties() {
            return Collections.emptyMap();
        }

        @Override
        public ConnectorScanRange withTableLevelRowCount(long c) {
            return new CountTrackingRange(id, c);
        }

        @Override
        public void populateRangeParams(TTableFormatFileDesc formatDesc,
                TFileRangeDesc rangeDesc) {
        }
    }

    @Test
    public void testThreeRangesGetEvenSplitWithRemainderOnLast() {
        List<ConnectorScanRange> ranges = Arrays.asList(
                new CountTrackingRange("a", -1L),
                new CountTrackingRange("b", -1L),
                new CountTrackingRange("c", -1L));
        List<ConnectorScanRange> out = PluginDrivenScanNode.applyCountToRanges(ranges, 100L);
        Assertions.assertEquals(3, out.size());
        Assertions.assertEquals(33L, ((CountTrackingRange) out.get(0)).getCount());
        Assertions.assertEquals(33L, ((CountTrackingRange) out.get(1)).getCount());
        Assertions.assertEquals(34L, ((CountTrackingRange) out.get(2)).getCount(),
                "remainder must land on the LAST range, matching legacy assignCountToSplits");
    }

    @Test
    public void testSingleRangeReceivesFullCount() {
        List<ConnectorScanRange> ranges = Collections.singletonList(
                new CountTrackingRange("only", -1L));
        List<ConnectorScanRange> out = PluginDrivenScanNode.applyCountToRanges(ranges, 777L);
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals(777L, ((CountTrackingRange) out.get(0)).getCount());
    }

    @Test
    public void testEvenlyDivisibleTotal() {
        List<ConnectorScanRange> ranges = Arrays.asList(
                new CountTrackingRange("a", -1L),
                new CountTrackingRange("b", -1L),
                new CountTrackingRange("c", -1L),
                new CountTrackingRange("d", -1L));
        List<ConnectorScanRange> out = PluginDrivenScanNode.applyCountToRanges(ranges, 200L);
        for (int i = 0; i < 4; i++) {
            Assertions.assertEquals(50L, ((CountTrackingRange) out.get(i)).getCount(),
                    "evenly divisible totals must produce equal per-range counts");
        }
    }

    @Test
    public void testZeroTotalProducesZeroPerRange() {
        List<ConnectorScanRange> ranges = Arrays.asList(
                new CountTrackingRange("a", -1L),
                new CountTrackingRange("b", -1L));
        List<ConnectorScanRange> out = PluginDrivenScanNode.applyCountToRanges(ranges, 0L);
        Assertions.assertEquals(0L, ((CountTrackingRange) out.get(0)).getCount());
        Assertions.assertEquals(0L, ((CountTrackingRange) out.get(1)).getCount());
    }

    @Test
    public void testEmptyRangesReturnsSameList() {
        List<ConnectorScanRange> ranges = Collections.emptyList();
        List<ConnectorScanRange> out = PluginDrivenScanNode.applyCountToRanges(ranges, 100L);
        Assertions.assertSame(ranges, out,
                "empty input must short-circuit without allocating a new list");
    }

    @Test
    public void testOriginalRangesAreNotMutated() {
        CountTrackingRange a = new CountTrackingRange("a", -1L);
        CountTrackingRange b = new CountTrackingRange("b", -1L);
        List<ConnectorScanRange> ranges = new ArrayList<>(Arrays.asList(a, b));
        PluginDrivenScanNode.applyCountToRanges(ranges, 50L);
        Assertions.assertEquals(-1L, a.getCount(),
                "withTableLevelRowCount must not mutate the original range");
        Assertions.assertEquals(-1L, b.getCount());
    }

    @Test
    public void testRangesWithoutCountSupportFallThroughUnchanged() {
        ConnectorScanRange opaque = new ConnectorScanRange() {
            private static final long serialVersionUID = 1L;

            @Override
            public ConnectorScanRangeType getRangeType() {
                return ConnectorScanRangeType.FILE_SCAN;
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public void populateRangeParams(TTableFormatFileDesc formatDesc,
                    TFileRangeDesc rangeDesc) {
            }
            // intentionally does NOT override withTableLevelRowCount —
            // default returns this, which is the documented opt-out.
        };
        List<ConnectorScanRange> out = PluginDrivenScanNode.applyCountToRanges(
                Collections.singletonList(opaque), 999L);
        Assertions.assertSame(opaque, out.get(0),
                "default withTableLevelRowCount returns this so non-iceberg ranges flow through unchanged");
    }
}
