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

package org.apache.doris.connector.credential;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * {@link CredentialResolver} reading the secret content from a file referenced
 * by {@code file:///absolute/path}. Rejects any non-absolute path or any path
 * containing a {@code ..} traversal segment.
 */
public final class FileCredentialResolver implements CredentialResolver {

    public static final String SCHEME = "file";

    @Override
    public String scheme() {
        return SCHEME;
    }

    @Override
    public Credential resolve(URI ref, ResolverContext ctx) {
        if (ref == null) {
            throw new CredentialResolutionException("file reference is null");
        }
        String rawPath = ref.getPath();
        if (rawPath == null || rawPath.trim().isEmpty()) {
            throw new CredentialResolutionException("file reference has empty path: " + ref);
        }
        Path path;
        try {
            path = Paths.get(ref);
        } catch (IllegalArgumentException e) {
            throw new CredentialResolutionException("file reference is invalid: " + ref, e);
        }
        if (!path.isAbsolute()) {
            throw new CredentialResolutionException("file reference must be absolute: " + ref);
        }
        for (Path segment : path) {
            if ("..".equals(segment.toString())) {
                throw new CredentialResolutionException(
                        "file reference must not contain '..' traversal: " + ref);
            }
        }
        byte[] content;
        try {
            content = Files.readAllBytes(path);
        } catch (IOException e) {
            throw new CredentialResolutionException("failed to read file credential at " + ref, e);
        }
        return Credential.ofBytes(content, null, null);
    }
}
