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

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Production {@link HudiTableLister} that walks an HMS catalog and
 * keeps the entries that look like Hudi tables — i.e. those whose
 * input format / serde / table parameters reference Hudi.
 *
 * <p>Errors during {@code listDatabases / listTables / getTable} are
 * logged and skipped so a single broken database cannot poison the
 * watcher's view of the rest of the catalog.
 */
public final class HmsHudiTableLister implements HudiTableLister {

    private static final Logger LOG = LogManager.getLogger(HmsHudiTableLister.class);

    private final HmsClient hmsClient;
    private final String catalogName;

    public HmsHudiTableLister(HmsClient hmsClient, String catalogName) {
        this.hmsClient = Objects.requireNonNull(hmsClient, "hmsClient");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    }

    @Override
    public List<HudiTableRef> listTables() {
        List<HudiTableRef> out = new ArrayList<>();
        List<String> dbs;
        try {
            dbs = hmsClient.listDatabases();
        } catch (RuntimeException e) {
            LOG.warn("hudi[{}] listDatabases failed: {}", catalogName, e.getMessage());
            return out;
        }
        for (String db : dbs) {
            List<String> tables;
            try {
                tables = hmsClient.listTables(db);
            } catch (RuntimeException e) {
                LOG.debug("hudi[{}] listTables({}) failed: {}", catalogName, db, e.getMessage());
                continue;
            }
            for (String t : tables) {
                HmsTableInfo info;
                try {
                    info = hmsClient.getTable(db, t);
                } catch (RuntimeException e) {
                    LOG.debug("hudi[{}] getTable({}.{}) failed: {}",
                            catalogName, db, t, e.getMessage());
                    continue;
                }
                if (looksLikeHudi(info) && info.getLocation() != null
                        && !info.getLocation().isEmpty()) {
                    out.add(new HudiTableRef(db, t, info.getLocation()));
                }
            }
        }
        return out;
    }

    private static boolean looksLikeHudi(HmsTableInfo info) {
        String inputFormat = info.getInputFormat();
        if (inputFormat != null
                && (inputFormat.contains("Hoodie") || inputFormat.contains("hoodie"))) {
            return true;
        }
        String serde = info.getSerializationLib();
        if (serde != null && serde.toLowerCase(Locale.ROOT).contains("hoodie")) {
            return true;
        }
        Map<String, String> params = info.getParameters();
        if (params != null) {
            String provider = params.get("spark.sql.sources.provider");
            if ("hudi".equalsIgnoreCase(provider)) {
                return true;
            }
        }
        return false;
    }
}
