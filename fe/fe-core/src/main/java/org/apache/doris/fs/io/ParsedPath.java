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

package org.apache.doris.fs.io;

import org.apache.doris.fs.Location;

import org.apache.hadoop.fs.Path;

/**
 * Temporary class to isolate the path parsing logic from the rest of the codebase.
 *
 * @deprecated Use {@link Location} instead. This class remains for backward compatibility.
 *             Use {@link #toLocation()} to convert to the new type,
 *             or {@link #fromLocation(Location)} to create from Location.
 */
@Deprecated
public class ParsedPath {
    private String origPath;

    public ParsedPath(String path) {
        this.origPath = path;
    }

    @Override
    public String toString() {
        return this.origPath;
    }

    public Path toHadoopPath() {
        return new Path(origPath);
    }

    /**
     * Converts this ParsedPath to a {@link Location}.
     *
     * @return a Location representing the same path
     */
    public Location toLocation() {
        return Location.of(origPath);
    }

    /**
     * Creates a ParsedPath from a {@link Location}.
     *
     * @param location the Location to convert
     * @return a new ParsedPath
     */
    public static ParsedPath fromLocation(Location location) {
        return new ParsedPath(location.toString());
    }
}
