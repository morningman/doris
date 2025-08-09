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

import org.apache.doris.backup.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.fs.StorageTypeMapper;
import org.apache.doris.fs.remote.S3FileSystem;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RestCatalogTest {

    // set your AWS access key and secret key
    private String awsAk = "";
    private String awsSk = "";
    private String awsAcountId = "";

    private String aliyunAk = "";
    private String aliyunSk = "";

    @Ignore
    public void testIcebergGlueRestCatalog() {
        Catalog glueRestCatalog = initIcebergGlueRestCatalog();
        SupportsNamespaces nsCatalog = (SupportsNamespaces) glueRestCatalog;
        // List namespaces and assert
        nsCatalog.listNamespaces(Namespace.empty()).forEach(namespace1 -> {
            System.out.println("Namespace: " + namespace1);
            Assertions.assertNotNull(namespace1, "Namespace should not be null");

            glueRestCatalog.listTables(namespace1).forEach(tableIdentifier -> {
                System.out.println("Table: " + tableIdentifier.name());
                Assertions.assertNotNull(tableIdentifier, "TableIdentifier should not be null");

                // Load table history and assert
                Table iceTable = glueRestCatalog.loadTable(tableIdentifier);
                iceTable.history().forEach(snapshot -> {
                    System.out.println("Snapshot: " + snapshot);
                    Assertions.assertNotNull(snapshot, "Snapshot should not be null");
                });

                CloseableIterable<FileScanTask> tasks = iceTable.newScan().planFiles();
                tasks.forEach(task -> {
                    System.out.println("FileScanTask: " + task);
                    Assertions.assertNotNull(task, "FileScanTask should not be null");
                });
            });
        });
    }

    private Catalog initIcebergGlueRestCatalog() {
        Map<String, String> options = Maps.newHashMap();
        options.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        options.put(CatalogProperties.URI, "https://glue.ap-east-1.amazonaws.com/iceberg");
        options.put(CatalogProperties.WAREHOUSE_LOCATION,
                awsAcountId + ":s3tablescatalog/s3-table-bucket-hk-glue-test");
        // remove this endpoint prop, or, add https://
        options.put(S3FileIOProperties.ENDPOINT, "https://s3.ap-east-1.amazonaws.com");
        // must set:
        // software.amazon.awssdk.core.exception.SdkClientException: Unable to load region from any of the providers in
        // the chain software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain@627ff1b8:
        // [software.amazon.awssdk.regions.providers.SystemSettingsRegionProvider@67d32a54:
        // Unable to load region from system settings. Region must be specified either via environment variable
        // (AWS_REGION) or  system property (aws.region).,
        // software.amazon.awssdk.regions.providers.AwsProfileRegionProvider@2792b416: No region provided in profile:
        // default, software.amazon.awssdk.regions.providers.InstanceProfileRegionProvider@5cff6b74:
        // Unable to contact EC2 metadata service.]
        options.put(AwsClientProperties.CLIENT_REGION, "ap-east-1");
        // Forbidden: {"message":"Missing Authentication Token"}
        options.put("rest.sigv4-enabled", "true");
        // Forbidden: {"message":"Credential should be scoped to correct service: 'glue'. "}
        options.put("rest.signing-name", "glue");
        //  Forbidden: {"message":"The security token included in the request is invalid."}
        options.put("rest.access-key-id", awsAk);
        // Forbidden: {"message":"The request signature we calculated does not match the signature you provided.
        // Check your AWS Secret Access Key and signing method. Consult the service documentation for details."}
        options.put("rest.secret-access-key", awsSk);
        // same as AwsClientProperties.CLIENT_REGION, "ap-east-1"
        options.put("rest.signing-region", "ap-east-1");
        // options.put("iceberg.catalog.warehouse", "<accountid>:s3tablescatalog/<table-bucket-name>");
        // 4. Build iceberg catalog
        Configuration conf = new Configuration();
        return CatalogUtil.buildIcebergCatalog("glue_test", options, conf);
    }

    @Test
    public void testPaimonDlfRestCatalog() throws DatabaseNotExistException, TableNotExistException, UserException {
        org.apache.paimon.catalog.Catalog catalog = initPaimonDlfRestCatalog();
        System.out.println(catalog);
        List<String> dbs = catalog.listDatabases();
        for (String dbName : dbs) {
            System.out.println("yy debug get db: " + dbName);
            Database db = catalog.getDatabase(dbName);
            System.out.println("yy debug get db instance: " + db.name() + ", " + db.options() + ", " + db.comment());
            List<String> tables = catalog.listTables(dbName);
            for (String tblName : tables) {
                System.out.println("yy debug get table: " + tblName);
                if (!tblName.equalsIgnoreCase("users_samples")) {
                    continue;
                }
                org.apache.paimon.table.Table table = catalog.getTable(
                        org.apache.paimon.catalog.Identifier.create(dbName, tblName));
                System.out.println("yy debug get table instance: " + table.name() + ", " + table.options() + ", "
                        + table.comment());

                FileIO fileIO = table.fileIO();
                if (fileIO instanceof RESTTokenFileIO) {
                    System.out.println("yy debug get file io instance: " + fileIO.getClass().getName());
                    RESTTokenFileIO restTokenFileIO = (RESTTokenFileIO) fileIO;
                    RESTToken restToken = restTokenFileIO.validToken();
                    Map<String, String> tokens = restToken.token();
                    for (Map.Entry<String, String> kv : tokens.entrySet()) {
                        System.out.println("yy debug get token: " + kv.getKey() + ", " + kv.getValue());
                    }
                    String accType = tokens.get("fs.oss.token.access.type");
                    String tmpAk = tokens.get("fs.oss.accessKeyId");
                    String tmpSk = tokens.get("fs.oss.accessKeySecret");
                    String stsToken = tokens.get("fs.oss.securityToken");
                    String endpoint = tokens.get("fs.oss.endpoint");

                    ReadBuilder readBuilder = table.newReadBuilder();
                    List<Split> paimonSplits = readBuilder.newScan().plan().splits();
                    for (Split split : paimonSplits) {
                        System.out.println("yy debug get split: " + split);
                        if (split instanceof DataSplit) {
                            DataSplit dataSplit = (DataSplit) split;
                            Optional<List<RawFile>> rawFiles = dataSplit.convertToRawFiles();
                            if (rawFiles.isPresent()) {
                                for (RawFile rawFile : rawFiles.get()) {
                                    System.out.println("yy debug get raw file: " + rawFile.path());
                                    readOssFile(rawFile.path(), tmpAk, tmpSk, stsToken, endpoint, "oss-cn-beijing");
                                    // readFile(rawFile.path(), tmpAk, tmpSk, stsToken, endpoint,
                                    //        "oss-cn-beijing");
                                    // readFileWithS3Client(rawFile.path(), tmpAk, tmpSk, stsToken, endpoint,
                                    //        "oss-cn-beijing");
                                }
                            } else {
                                System.out.println("yy debug no raw files in this data split");
                            }
                        }
                    }
                } else {
                    System.out.println("yy debug fileIO is not RESTTokenFileIO, it is: " + fileIO.getClass().getName());
                }
            }
        }
    }

    private void readOssFile(String path, String tmpAk, String tmpSk, String stsToken, String endpoint, String region) {
        // replace "oss://" with "s3://"
        String finalPath = path.startsWith("oss://") ? path.replace("oss://", "s3://") : path;
        System.out.println("yy debug final path: " + finalPath);
        Map<String, String> props = Maps.newHashMap();
        props.put("s3.endpoint", endpoint);
        props.put("s3.region", region);
        props.put("s3.access_key", tmpAk);
        props.put("s3.secret_key", tmpSk);
        props.put("s3.session_token", stsToken);
        S3Properties s3Properties = S3Properties.of(props);
        S3FileSystem s3Fs = (S3FileSystem) StorageTypeMapper.create(s3Properties);
        File localFile = new File("/tmp/s3/" + System.currentTimeMillis() + ".data");
        if (localFile.exists()) {
            try {
                Files.delete(localFile.toPath());
            } catch (IOException e) {
                System.err.println("Failed to delete existing local file: " + localFile.getAbsolutePath());
                e.printStackTrace();
            }
        } else {
            localFile.getParentFile().mkdirs(); // Ensure parent directories exist
        }
        Status st = s3Fs.getObjStorage().getObject(finalPath, localFile);
        System.out.println(st);
        if (st.ok()) {
            System.out.println("yy debug local path: " + localFile.getAbsolutePath());
        }
    }

    /**
     * CREATE CATALOG `paimon-rest-catalog`
     * WITH (
     * 'type' = 'paimon',
     * 'uri' = '<catalog server url>',
     * 'metastore' = 'rest',
     * 'warehouse' = 'my_instance_name',
     * 'token.provider' = 'dlf',
     * 'dlf.access-key-id'='<access-key-id>',
     * 'dlf.access-key-secret'='<access-key-secret>',
     * );
     *
     * @return
     */
    private org.apache.paimon.catalog.Catalog initPaimonDlfRestCatalog() {
        HiveConf hiveConf = new HiveConf();
        Options catalogOptions = new Options();
        catalogOptions.set("metastore", "rest");
        catalogOptions.set("warehouse", "new_dfl_paimon_catalog");
        catalogOptions.set("uri", "http://cn-beijing-vpc.dlf.aliyuncs.com");
        catalogOptions.set("token.provider", "dlf");
        catalogOptions.set("dlf.access-key-id", aliyunAk);
        catalogOptions.set("dlf.access-key-secret", aliyunSk);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions, hiveConf);
        return CatalogFactory.createCatalog(catalogContext);
    }

    private void readFile(String filePath, String accessKeyId, String secretAccessKey,
            String sessionToken, String endpoint, String region) throws UserException {
        // 创建STS临时凭证
        BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                accessKeyId,
                secretAccessKey,
                sessionToken
        );

        // 配置客户端
        ClientConfiguration clientConfig = new ClientConfiguration();
        // 阿里云OSS需要使用V4签名
        clientConfig.setSignerOverride("AWSS3V4SignerType");

        // 构建S3客户端
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                .withEndpointConfiguration(new EndpointConfiguration(endpoint, region))
                .withClientConfiguration(clientConfig)
                // 对于阿里云OSS，需要禁用路径风格访问
                .withPathStyleAccessEnabled(false)
                .build();

        S3URI s3URI = S3URI.create(filePath);
        System.out.println("yy debug s3uri: " + s3URI);
        try {
            // 示例：从OSS下载文件并读取内容
            String content = downloadAndReadFile(s3Client, s3URI.getBucket(), s3URI.getKey());
            System.out.println("文件内容: " + content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String downloadAndReadFile(AmazonS3 s3Client, String bucketName, String objectKey) throws IOException {
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, objectKey));
        try (InputStream inputStream = s3Object.getObjectContent();
                InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            StringBuilder content = new StringBuilder();
            char[] buffer = new char[1024];
            int bytesRead;
            while ((bytesRead = reader.read(buffer)) != -1) {
                content.append(buffer, 0, bytesRead);
            }
            return content.toString();
        }
    }

    private void readFileWithS3Client(String path, String tmpAk, String tmpSk, String stsToken, String endpoint,
            String region) {
        // 3. 配置S3Client（核心：适配阿里云OSS）
        S3Client s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsSessionCredentials.create(tmpAk, tmpSk,
                        stsToken)))
                .region(Region.of(region)) // 阿里云不验证Region，随意填写一个有效Region即可
                .endpointOverride(URI.create("https://" + endpoint)) // 阿里云OSS端点
                .serviceConfiguration(S3Configuration.builder()
                        .chunkedEncodingEnabled(false)
                        .pathStyleAccessEnabled(false)
                        .build())
                .build();
        try {
            S3URI s3URI = S3URI.create(path);
            System.out.println("yy debug s3uri: " + s3URI);
            downloadObjectWithS3Client(s3Client, s3URI.getBucket(), s3URI.getKey());
        } catch (UserException e) {
            throw new RuntimeException(e);
        } finally {
            // 关闭客户端
            s3Client.close();
        }
    }

    private void downloadObjectWithS3Client(S3Client s3Client, String bucketName, String objectKey) {
        software.amazon.awssdk.services.s3.model.GetObjectRequest request
                = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();

        try (ResponseInputStream inputStream = s3Client.getObject(request);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line); // 逐行打印内容
            }
        } catch (IOException e) {
            System.err.println("读取文件内容失败：" + e.getMessage());
            e.printStackTrace();
        }
    }
}
