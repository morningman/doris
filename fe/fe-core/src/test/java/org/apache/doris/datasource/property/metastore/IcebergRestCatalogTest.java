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

package org.apache.doris.datasource.property.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Disabled("Disabled until AWS credentials are available")
public class IcebergRestCatalogTest {

    private Catalog catalog;
    private SupportsNamespaces nsCatalog;
    private AWSGlueProperties glueProperties;
    private static final Namespace queryNameSpace = Namespace.of("test"); // Replace with your namespace
    private static final String AWS_ACCESS_KEY_ID = "ak"; // Replace with actual access key
    private static final String AWS_SECRET_ACCESS_KEY = "sk/lht4rMIfbhVftujO2alFx";
    // Replace with actual secret key
    // private static final String AWS_GLUE_ENDPOINT = "https://glue.ap-northeast-1.amazonaws.com"; // Replace with your endpoint
    private static final String AWS_GLUE_ENDPOINT = "https://glue.us-west-1.amazonaws.com";
            // Replace with your endpoint

    @BeforeEach
    public void setUp() {

        // https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue-endpoint.html
        // Setup properties
        Map<String, String> props = new HashMap<>();
        // Use environment variables for sensitive keys
        // "rest.sigv4-enabled": "true",
        // "rest.signing-name": "glue",
        // "rest.signing-region": region
        props.put("rest.sigv4-enabled", "true");
        props.put("rest.signing-name", "s3tables");
        props.put("rest.signing-region", "us-west-1");
        props.put("type", "rest");
        props.put("iceberg.catalog.type", "rest");
        // props.put("uri", "https://glue.us-west-1.amazonaws.com/iceberg");
        props.put("uri", "https://s3tables.us-west-1.amazonaws.com/iceberg");
        props.put("client.credentials-provider",
                "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x");
        props.put("client.credentials-provider.glue.access_key", AWS_ACCESS_KEY_ID);
        props.put("client.credentials-provider.glue.secret_key", AWS_SECRET_ACCESS_KEY);

        Configuration conf = new Configuration();

        catalog = CatalogUtil.buildIcebergCatalog("test", props, conf);
        nsCatalog = (SupportsNamespaces) catalog;
    }

    @Test
    public void testConnection() {
        // Check if catalog can be initialized without errors
        Assertions.assertNotNull(catalog, "Glue Catalog should be initialized");
        // Ensure at least one namespace exists
        Assertions.assertFalse(nsCatalog.listNamespaces(Namespace.empty()).isEmpty(),
                "Namespace list should not be empty");
    }

    @Test
    public void testListNamespaces() {
        // List namespaces and assert
        nsCatalog.listNamespaces(Namespace.empty()).forEach(namespace1 -> {
            System.out.println("Namespace: " + namespace1);
            Assertions.assertNotNull(namespace1, "Namespace should not be null");
        });
    }

    @Test
    public void testListTables() {
        // List tables in a given namespace
        catalog.listTables(queryNameSpace).forEach(tableIdentifier -> {
            System.out.println("Table: " + tableIdentifier.name());
            Assertions.assertNotNull(tableIdentifier, "TableIdentifier should not be null");

            // Load table history and assert
            catalog.loadTable(tableIdentifier).history().forEach(snapshot -> {
                System.out.println("Snapshot: " + snapshot);
                Assertions.assertNotNull(snapshot, "Snapshot should not be null");
            });
        });
    }

    @AfterEach
    public void tearDown() throws IOException {
    }
}
