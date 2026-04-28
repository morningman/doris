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

package org.apache.doris.connector.iceberg.cache;

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

import java.util.Objects;

/**
 * Cache key for {@link IcebergPluginManifestCache}. The key is intentionally
 * <b>not table-scoped</b> — manifest files in iceberg are immutable and
 * shared across snapshots and (in some catalogs) across tables, so a key
 * scoped only to {@code (catalogName, manifestPath, content)} maximises
 * reuse without sacrificing correctness.
 *
 * <p>The {@code content} discriminator (DATA vs DELETES) is included
 * because the cached value-type differs between the two: a delete
 * manifest is decoded with {@code ManifestFiles.readDeleteManifest}
 * (which requires the table's partition specs) whereas a data manifest
 * is decoded with {@code ManifestFiles.read}.
 */
public final class ManifestCacheKey {

    private final String catalogName;
    private final String manifestPath;
    private final ManifestContent content;

    public ManifestCacheKey(String catalogName, String manifestPath, ManifestContent content) {
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
        this.manifestPath = Objects.requireNonNull(manifestPath, "manifestPath");
        this.content = Objects.requireNonNull(content, "content");
    }

    public static ManifestCacheKey of(String catalogName, ManifestFile manifest) {
        Objects.requireNonNull(manifest, "manifest");
        return new ManifestCacheKey(catalogName, manifest.path(), manifest.content());
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getManifestPath() {
        return manifestPath;
    }

    public ManifestContent getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ManifestCacheKey)) {
            return false;
        }
        ManifestCacheKey that = (ManifestCacheKey) o;
        return catalogName.equals(that.catalogName)
                && manifestPath.equals(that.manifestPath)
                && content == that.content;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, manifestPath, content);
    }

    @Override
    public String toString() {
        return "ManifestCacheKey{catalog=" + catalogName
                + ", path=" + manifestPath
                + ", content=" + content + '}';
    }
}
