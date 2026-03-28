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

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class LocationTest {

    @Test
    public void testBasicParsing() {
        Location loc = Location.of("s3://bucket/path/to/file.parquet");
        Assert.assertEquals(Optional.of("s3"), loc.scheme());
        Assert.assertEquals(Optional.of("bucket"), loc.host());
        Assert.assertEquals("path/to/file.parquet", loc.path());
        Assert.assertEquals("s3://bucket/path/to/file.parquet", loc.toString());
    }

    @Test
    public void testHdfsUri() {
        Location loc = Location.of("hdfs://namenode:8020/warehouse/data");
        Assert.assertEquals(Optional.of("hdfs"), loc.scheme());
        Assert.assertEquals(Optional.of("namenode:8020"), loc.host());
        Assert.assertEquals("warehouse/data", loc.path());
    }

    @Test
    public void testObjectStorageMethods() {
        Location loc = Location.of("s3://my-bucket/prefix/key/file.dat");
        Assert.assertEquals("my-bucket", loc.bucket());
        Assert.assertEquals("prefix/key/file.dat", loc.key());
    }

    @Test
    public void testFileName() {
        Location loc = Location.of("s3://bucket/path/to/file.parquet");
        Assert.assertEquals("file.parquet", loc.fileName());
    }

    @Test
    public void testParentDirectory() {
        Location loc = Location.of("s3://bucket/path/to/file.parquet");
        Location parent = loc.parentDirectory();
        Assert.assertEquals("s3://bucket/path/to", parent.toString());
    }

    @Test
    public void testAppendPath() {
        Location dir = Location.of("s3://bucket/prefix");
        Location child = dir.appendPath("subdir/file.txt");
        Assert.assertEquals("s3://bucket/prefix/subdir/file.txt", child.toString());
    }

    @Test
    public void testEqualsAndHashCode() {
        Location a = Location.of("s3://bucket/key");
        Location b = Location.of("s3://bucket/key");
        Location c = Location.of("s3://bucket/other");

        Assert.assertEquals(a, b);
        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testToHadoopPath() {
        Location loc = Location.of("hdfs://namenode:8020/warehouse/data");
        org.apache.hadoop.fs.Path hadoopPath = loc.toHadoopPath();
        Assert.assertEquals("hdfs://namenode:8020/warehouse/data", hadoopPath.toString());
    }

    @Test
    public void testPathOnlyLocation() {
        Location loc = Location.of("/local/path/to/file");
        Assert.assertEquals(Optional.empty(), loc.scheme());
        Assert.assertEquals(Optional.empty(), loc.host());
        Assert.assertEquals("/local/path/to/file", loc.path());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullUri() {
        Location.of(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyUri() {
        Location.of("");
    }

    @Test
    public void testGcsUri() {
        Location loc = Location.of("gs://gcs-bucket/data/file.orc");
        Assert.assertEquals(Optional.of("gs"), loc.scheme());
        Assert.assertEquals("gcs-bucket", loc.bucket());
        Assert.assertEquals("data/file.orc", loc.key());
    }

    @Test
    public void testSibling() {
        Location file = Location.of("s3://bucket/prefix/old.txt");
        Location sibling = file.sibling("new.txt");
        Assert.assertEquals("s3://bucket/prefix/new.txt", sibling.toString());
    }

    @Test
    public void testAppendSuffix() {
        Location loc = Location.of("s3://bucket/path/file");
        Location withSuffix = loc.appendSuffix(".parquet");
        Assert.assertEquals("s3://bucket/path/file.parquet", withSuffix.toString());
    }

    @Test
    public void testVerifyValidFileLocation() {
        Location valid = Location.of("s3://bucket/path/file.txt");
        valid.verifyValidFileLocation(); // should not throw
    }

    @Test(expected = IllegalStateException.class)
    public void testVerifyInvalidFileLocationTrailingSlash() {
        Location invalid = Location.of("s3://bucket/path/dir/");
        invalid.verifyValidFileLocation();
    }
}
