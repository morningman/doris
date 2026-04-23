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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileCredentialResolverTest {

    private final FileCredentialResolver resolver = new FileCredentialResolver();
    private static final ResolverContext CTX = new ResolverContext("test_catalog", 60L);

    @Test
    public void scheme_isFile() {
        Assertions.assertEquals("file", resolver.scheme());
    }

    @Test
    public void resolve_absolutePath_returnsContent(@TempDir Path tmp) throws Exception {
        Path f = tmp.resolve("token.txt");
        Files.write(f, "hello-secret".getBytes(StandardCharsets.UTF_8));
        Credential c = resolver.resolve(f.toUri(), CTX);
        Assertions.assertEquals("hello-secret", c.secretAsString());
    }

    @Test
    public void resolve_relativePath_throws() {
        URI rel = URI.create("file:relative/path");
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> resolver.resolve(rel, CTX));
    }

    @Test
    public void resolve_traversal_throws(@TempDir Path tmp) throws Exception {
        Path nested = tmp.resolve("dir");
        Files.createDirectories(nested);
        Path target = tmp.resolve("token.txt");
        Files.write(target, "hello".getBytes(StandardCharsets.UTF_8));
        URI traversal = URI.create("file://" + nested.toAbsolutePath() + "/../token.txt");
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> resolver.resolve(traversal, CTX));
    }

    @Test
    public void resolve_missingFile_throws(@TempDir Path tmp) {
        URI missing = tmp.resolve("nope.txt").toUri();
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> resolver.resolve(missing, CTX));
    }

    @Test
    public void resolve_nullRef_throws() {
        Assertions.assertThrows(CredentialResolutionException.class,
                () -> resolver.resolve(null, CTX));
    }
}
