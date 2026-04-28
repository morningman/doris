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

package org.apache.iceberg;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Plugin-internal façade over iceberg-core types whose JVM access
 * modifier is package-private (notably {@link DeleteFileIndex}). Lives
 * inside {@code org.apache.iceberg} so that this module can compile
 * against iceberg-core 1.10.1 without resorting to reflection.
 *
 * <p>Used exclusively by
 * {@code org.apache.doris.connector.iceberg.source.IcebergScanPlanProvider#planFileScanTaskWithManifestCache}
 * to build sequence-aware delete-file -> data-file lookups when the
 * plugin manifest cache is enabled. Mirrors the legacy fe-core
 * {@code IcebergScanNode#planFileScanTaskWithManifestCache} algorithm.
 */
public final class IcebergPluginInternals {

    private IcebergPluginInternals() {
    }

    /**
     * Wraps {@link DeleteFileIndex.Builder} to expose the public
     * {@code forDataFile(seq, dataFile)} contract over a builder
     * configured with {@code specsById} and {@code caseSensitive}.
     */
    public static DeleteFileLookup buildDeleteFileLookup(
            Iterable<DeleteFile> deleteFiles,
            Map<Integer, PartitionSpec> specsById,
            boolean caseSensitive) {
        DeleteFileIndex index = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(specsById)
                .caseSensitive(caseSensitive)
                .build();
        return (sequenceNumber, dataFile) ->
                Arrays.asList(index.forDataFile(sequenceNumber, dataFile));
    }

    /** Functional view of {@link DeleteFileIndex#forDataFile}. */
    @FunctionalInterface
    public interface DeleteFileLookup {
        List<DeleteFile> forDataFile(long dataFileSequenceNumber, DataFile dataFile);
    }

    /** Port of legacy {@code IcebergUtils#getMatchingManifest}: prune
     *  data manifests by partition filter and skip empty manifests. */
    public static CloseableIterable<ManifestFile> matchingDataManifests(
            List<ManifestFile> dataManifests,
            Map<Integer, PartitionSpec> specsById,
            Expression dataFilter) {
        Map<Integer, ManifestEvaluator> evaluators = new HashMap<>();
        CloseableIterable<ManifestFile> matching = CloseableIterable.filter(
                CloseableIterable.withNoopClose(dataManifests),
                m -> evaluators.computeIfAbsent(m.partitionSpecId(), id -> {
                    PartitionSpec spec = specsById.get(id);
                    return ManifestEvaluator.forPartitionFilter(
                            Expressions.and(Expressions.alwaysTrue(),
                                    Projections.inclusive(spec, true).project(dataFilter)),
                            spec, true);
                }).eval(m));
        return CloseableIterable.filter(matching,
                m -> m.hasAddedFiles() || m.hasExistingFiles());
    }
}
