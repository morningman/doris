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

package org.apache.doris.connector.hudi.event;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Production {@link HudiTimelineLister} backed by Hadoop
 * {@link FileSystem}. Lists the {@code .hoodie/} directory and parses
 * each filename into a {@link HudiInstant}.
 *
 * <p>Recognised filename patterns:
 * <ul>
 *   <li>{@code <ts>.commit / .deltacommit / .replacecommit / .clean /
 *       .rollback / .savepoint} → COMPLETED;</li>
 *   <li>{@code <ts>.<action>.requested} → REQUESTED;</li>
 *   <li>{@code <ts>.<action>.inflight} → INFLIGHT.</li>
 * </ul>
 *
 * <p>Schema-hash extraction from commit metadata is intentionally
 * deferred: the .commit JSON parser would require the hudi-common
 * commit-metadata classes and adds non-trivial IO per tick. The
 * translator already exposes the {@code TableAltered} branch so a
 * future enhancement (M3) can populate {@code schemaHash} without
 * changing the SPI surface.
 */
public final class FsHudiTimelineLister implements HudiTimelineLister {

    private static final String HOODIE_DIR = ".hoodie";

    private final Configuration conf;

    public FsHudiTimelineLister(Configuration conf) {
        this.conf = Objects.requireNonNull(conf, "conf");
    }

    @Override
    public List<HudiInstant> listInstants(HudiTableRef ref) {
        Objects.requireNonNull(ref, "ref");
        Path hoodieDir = new Path(ref.basePath(), HOODIE_DIR);
        FileSystem fs;
        try {
            fs = hoodieDir.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException("failed to open FileSystem for " + hoodieDir, e);
        }
        FileStatus[] entries;
        try {
            if (!fs.exists(hoodieDir)) {
                return Collections.emptyList();
            }
            entries = fs.listStatus(hoodieDir);
        } catch (IOException e) {
            throw new RuntimeException("failed to list " + hoodieDir, e);
        }
        if (entries == null || entries.length == 0) {
            return Collections.emptyList();
        }
        List<HudiInstant> out = new ArrayList<>(entries.length);
        for (FileStatus s : entries) {
            if (s.isDirectory()) {
                continue;
            }
            Optional<HudiInstant> parsed = parse(s.getPath().getName());
            parsed.ifPresent(out::add);
        }
        return out;
    }

    /**
     * Parse a Hudi instant filename. Returns {@link Optional#empty()}
     * for unrecognised files (e.g. {@code hoodie.properties},
     * {@code .schema}, {@code archived/}).
     */
    static Optional<HudiInstant> parse(String filename) {
        Objects.requireNonNull(filename, "filename");
        if (filename.startsWith("hoodie.") || filename.startsWith(".")) {
            return Optional.empty();
        }
        int firstDot = filename.indexOf('.');
        if (firstDot <= 0 || firstDot == filename.length() - 1) {
            return Optional.empty();
        }
        String ts = filename.substring(0, firstDot);
        String tail = filename.substring(firstDot + 1);
        // tail is "<action>" or "<action>.requested" / "<action>.inflight"
        String action;
        String state;
        int dot = tail.indexOf('.');
        if (dot < 0) {
            action = tail;
            state = "COMPLETED";
        } else {
            action = tail.substring(0, dot);
            String suffix = tail.substring(dot + 1).toLowerCase(Locale.ROOT);
            switch (suffix) {
                case "requested":
                    state = "REQUESTED";
                    break;
                case "inflight":
                    state = "INFLIGHT";
                    break;
                default:
                    return Optional.empty();
            }
        }
        if (action.isEmpty() || ts.chars().anyMatch(c -> !Character.isDigit(c))) {
            return Optional.empty();
        }
        return Optional.of(HudiInstant.of(ts, action, state));
    }
}
