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

package org.apache.doris.fs;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable file/directory location identifier.
 *
 * <p>Format: {@code scheme://[userInfo@]host[:port]/path}
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code s3://bucket/prefix/file.parquet}</li>
 *   <li>{@code hdfs://nameservice1/user/doris/backup}</li>
 *   <li>{@code abfss://container@account.dfs.core.windows.net/dir/file}</li>
 *   <li>{@code /local/path}</li>
 * </ul>
 *
 * <p>Differences from Trino Location:
 * <ul>
 *   <li>Provides {@link #bucket()} / {@link #key()} convenience methods for object storage</li>
 *   <li>Retains {@link #toHadoopPath()} for the migration period</li>
 * </ul>
 *
 * <p>Immutable and thread-safe.
 */
public final class Location {

    private final String location; // original complete string

    // Lazily parsed and cached fields
    private volatile boolean parsed;
    private String scheme;    // nullable: "s3", "hdfs", "abfss", etc.
    private String authority; // nullable: host[:port] or bucket or userInfo@host
    private String path;      // path portion: may or may not start with /

    // ====== Construction ======

    private Location(String location) {
        Preconditions.checkArgument(location != null && !location.isEmpty(),
                "location is null or empty");
        // Do not allow whitespace-only strings
        Preconditions.checkArgument(!location.trim().isEmpty(),
                "location is blank");
        this.location = location;
    }

    /**
     * Creates a Location from the given URI string.
     *
     * @param location a URI string such as {@code s3://bucket/key} or {@code /local/path}
     * @return a new Location
     * @throws IllegalArgumentException if location is null or empty
     */
    public static Location of(String location) {
        return new Location(location);
    }

    // ====== Lazy parsing ======

    private void ensureParsed() {
        if (!parsed) {
            synchronized (this) {
                if (!parsed) {
                    doParse();
                    parsed = true;
                }
            }
        }
    }

    private void doParse() {
        String remaining = location;

        // 1. Extract scheme
        int colonSlashSlash = remaining.indexOf("://");
        if (colonSlashSlash >= 0) {
            this.scheme = remaining.substring(0, colonSlashSlash);
            remaining = remaining.substring(colonSlashSlash + 3);

            // 2. Extract authority (everything before the first '/')
            int firstSlash = remaining.indexOf('/');
            if (firstSlash >= 0) {
                this.authority = remaining.substring(0, firstSlash);
                this.path = remaining.substring(firstSlash + 1); // strip the leading '/'
            } else {
                // No slash after authority, e.g. "s3://bucket"
                this.authority = remaining;
                this.path = "";
            }
        } else {
            // No scheme — local path
            this.scheme = null;
            this.authority = null;
            this.path = remaining;
        }
    }

    // ====== Query ======

    /**
     * Returns the scheme (e.g. "s3", "hdfs", "abfss"), or empty if no scheme.
     */
    public Optional<String> scheme() {
        ensureParsed();
        return Optional.ofNullable(scheme);
    }

    /**
     * Returns the authority portion (host[:port] or bucket), or empty if not present.
     */
    public Optional<String> host() {
        ensureParsed();
        return Optional.ofNullable(authority);
    }

    /**
     * Returns the path portion of the location.
     * For URIs with a scheme, this is the part after {@code scheme://authority/}.
     * For local paths, this is the entire string.
     */
    public String path() {
        ensureParsed();
        return path;
    }

    /**
     * Returns the file name — the part after the last '/'.
     * Returns the entire path if no '/' is present.
     */
    public String fileName() {
        ensureParsed();
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash >= 0) {
            return path.substring(lastSlash + 1);
        }
        return path;
    }

    // ====== Navigation ======

    /**
     * Returns the parent directory of this location.
     *
     * @throws IllegalStateException if this location has no parent (e.g. root)
     */
    public Location parentDirectory() {
        ensureParsed();
        String p = path;
        // Remove trailing slash if present
        if (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        int lastSlash = p.lastIndexOf('/');
        if (lastSlash < 0) {
            // Path is something like "file.txt" under the authority root
            if (scheme != null) {
                return Location.of(scheme + "://" + (authority != null ? authority : "") + "/");
            }
            throw new IllegalStateException("Location has no parent: " + location);
        }
        String parentPath = p.substring(0, lastSlash);
        if (scheme != null) {
            return Location.of(scheme + "://" + (authority != null ? authority : "") + "/" + parentPath);
        }
        return Location.of(parentPath);
    }

    /**
     * Appends a path element separated by '/'.
     *
     * @param element the path element to append
     * @return a new Location with the element appended
     */
    public Location appendPath(String element) {
        Preconditions.checkArgument(element != null && !element.isEmpty(),
                "path element is null or empty");
        String base = location;
        if (base.endsWith("/")) {
            return Location.of(base + element);
        }
        return Location.of(base + "/" + element);
    }

    /**
     * Appends a suffix directly (no '/' separator).
     *
     * @param suffix the suffix to append
     * @return a new Location with the suffix appended
     */
    public Location appendSuffix(String suffix) {
        Preconditions.checkArgument(suffix != null && !suffix.isEmpty(),
                "suffix is null or empty");
        return Location.of(location + suffix);
    }

    /**
     * Returns a sibling location — same parent directory, different name.
     *
     * @param name the new file/directory name
     * @return a new Location with the given name under the same parent
     */
    public Location sibling(String name) {
        return parentDirectory().appendPath(name);
    }

    // ====== Validation ======

    /**
     * Verifies this is a valid file location (path is non-empty and does not end with '/').
     *
     * @throws IllegalStateException if this is not a valid file location
     */
    public void verifyValidFileLocation() {
        ensureParsed();
        Preconditions.checkState(!path.isEmpty(),
                "Location path is empty: %s", location);
        Preconditions.checkState(!path.endsWith("/"),
                "File location path must not end with '/': %s", location);
    }

    // ====== Object storage convenience methods ======

    /**
     * Extracts the bucket name from the authority (e.g. for {@code s3://bucket/key},
     * returns "bucket").
     *
     * @return the bucket name
     * @throws IllegalStateException if no authority is present
     */
    public String bucket() {
        ensureParsed();
        Preconditions.checkState(authority != null && !authority.isEmpty(),
                "Location has no bucket/authority: %s", location);
        // For schemes like abfss://container@account.dfs.core.windows.net/path,
        // the bucket is the container part before '@'
        int atSign = authority.indexOf('@');
        if (atSign >= 0) {
            return authority.substring(0, atSign);
        }
        return authority;
    }

    /**
     * Returns the object key, which is the path portion for object storage.
     *
     * @return the object key
     */
    public String key() {
        ensureParsed();
        return path;
    }

    // ====== Transition compatibility ======

    /**
     * Converts this Location to a Hadoop Path.
     *
     * @return a Hadoop Path
     * @deprecated Use Location API instead of Hadoop Path
     */
    @Deprecated
    public org.apache.hadoop.fs.Path toHadoopPath() {
        return new org.apache.hadoop.fs.Path(location);
    }

    // ====== Standard methods ======

    /**
     * Returns the original location string.
     */
    @Override
    public String toString() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Location that = (Location) o;
        return Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(location);
    }
}
