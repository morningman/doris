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

package org.apache.doris.connector;

import org.apache.doris.connector.spi.ConnectorStorageView;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * FIX-URI-NORMALIZE fe-core bridge test: pins that the storage view resolved by
 * {@link DefaultConnectorContext#resolveStorage} rewrites a connector-supplied storage URI to
 * BE's canonical {@code s3://} scheme using the catalog's storage properties (the same
 * {@code LocationPath} normalization legacy {@code PaimonScanNode} applies via the 2-arg
 * {@code LocationPath.of(path, storagePropertiesMap)}). The paimon connector cannot import that
 * machinery, so this hook is its only access; without it a native ORC/Parquet read on an
 * OSS/COS/OBS warehouse reaches BE with an un-openable {@code oss://} path (data file fails, or a
 * deletion vector is silently dropped). FAILS before the fix (the SPI default view is a no-op
 * returning the raw URI).
 */
public class DefaultConnectorContextNormalizeUriTest {

    private static final Supplier<ExecutionAuthenticator> NOOP_AUTH =
            () -> new ExecutionAuthenticator() {};

    /** A context whose storage-props supplier yields a real OSS storage-properties map, built with
     *  the same {@code StorageAdapter.ofAll} machinery a real OSS catalog uses. */
    private static DefaultConnectorContext ossContext() throws Exception {
        Map<String, String> oss = new HashMap<>();
        oss.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        oss.put("oss.access_key", "ak");
        oss.put("oss.secret_key", "sk");
        List<StorageAdapter> all = StorageAdapter.ofAll(oss);
        Map<StorageTypeId, StorageAdapter> map = all.stream()
                .collect(Collectors.toMap(StorageAdapter::getType, Function.identity(), (a, b) -> a));
        return new DefaultConnectorContext("c", 1L, NOOP_AUTH, () -> map);
    }

    @Test
    public void normalizesOssSchemeToS3() throws Exception {
        // WHY: BE's scheme-dispatched S3 file factory only recognizes s3://; legacy LocationPath.of
        // rewrites oss:// (and cos/obs/s3a) -> s3://. This hook is the connector's ONLY access to that
        // normalization (it must not import LocationPath). MUTATION: returning the raw oss:// path
        // (the no-op SPI default view) -> red.
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet",
                ossContext().resolveStorage(null).normalizeUri("oss://bkt/warehouse/db/t/part-0.parquet"));
    }

    @Test
    public void s3SchemeIsUnchanged() throws Exception {
        // WHY: an already-canonical s3:// path must pass through unchanged (idempotent fast path).
        // MUTATION: mangling the s3:// path -> red.
        Assertions.assertEquals("s3://bkt/warehouse/f.parquet",
                ossContext().resolveStorage(null).normalizeUri("s3://bkt/warehouse/f.parquet"));
    }

    @Test
    public void nullOrBlankIsReturnedUnchanged() throws Exception {
        // WHY: defensive short-circuit before touching the storage-props supplier -> no NPE on a
        // null/blank path. MUTATION: NPE, or fabricating output from nothing -> red.
        Assertions.assertNull(ossContext().resolveStorage(null).normalizeUri(null));
        Assertions.assertEquals("", ossContext().resolveStorage(null).normalizeUri(""));
    }

    @Test
    public void failsLoudWhenNoStoragePropertiesForScheme() {
        // WHY: a context with no storage-properties map must FAIL LOUD on a real path rather than
        // silently shipping the raw oss:// to BE (which would corrupt reads). Mirrors legacy
        // LocationPath.ofAdapters(path, {}) throwing StoragePropertiesException. The ctors that do not wire a
        // storage map are never used by paimon, but the fail-loud contract is pinned here.
        // MUTATION: swallowing the error and returning the raw path -> red.
        DefaultConnectorContext noStorage = new DefaultConnectorContext("c", 1L);
        Assertions.assertThrows(RuntimeException.class,
                () -> noStorage.resolveStorage(null).normalizeUri("oss://bkt/a/part-0.parquet"));
    }

    // ---- FIX-REST-VENDED-URI-NORMALIZE (P9-1): the vended token resolves the view for a REST
    //      catalog, which is the ONLY storage map it has (its static map is empty). ----

    /** The raw per-table OSS vended token shape a REST catalog returns (mirrors
     *  DefaultConnectorContextVendTest / PaimonVendedCredentialsProviderTest). */
    private static Map<String, String> ossVendedToken() {
        Map<String, String> token = new HashMap<>();
        token.put("fs.oss.accessKeyId", "STS.testAccessKey123");
        token.put("fs.oss.accessKeySecret", "testSecretKey456");
        token.put("fs.oss.securityToken", "testSessionToken789");
        token.put("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        return token;
    }

    @Test
    public void vendedRestCredentialsNormalizeUnderEmptyStaticMap() {
        // THE BUG (P9-1, BLOCKER): a REST catalog's static storage map is EMPTY by design (vended creds
        // are per-table/dynamic), so the static-only path throws "No storage properties found for schema:
        // oss" on a native ORC/Parquet read — the exact corner DV-025 deferred but never closed. The
        // vended token resolves the view instead (legacy VendedCredentialsFactory: the vended map
        // REPLACES the empty static map). MUTATION: ignoring the token (static-only) -> throws -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L); // empty static map = REST
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet",
                restCtx.resolveStorage(ossVendedToken())
                        .normalizeUri("oss://bkt/warehouse/db/t/part-0.parquet"));
    }

    @Test
    public void vendedAdlsCredentialsKeepAbfsPathAndUseHdfsReader() {
        String path = "abfss://container@account.dfs.core.windows.net/table/part-0.parquet";
        Map<String, String> token = Map.of(
                "adls.sas-token.account.dfs.core.windows.net", "testSasToken",
                "adls.sas-token-expires-at-ms.account.dfs.core.windows.net", "4102444800000");
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);

        // ONE view answers both: the abfss path stays intact AND selects the BE hadoop reader —
        // the pair must agree, or BE opens the wrong client for the path it is given.
        ConnectorStorageView view = restCtx.resolveStorage(token);
        Assertions.assertEquals(path, view.normalizeUri(path));
        Assertions.assertEquals(TFileType.FILE_HDFS.name(), view.backendFileType(path));
    }

    @Test
    public void emptyTokenUnderEmptyStaticStillFailsLoud() {
        // WHY: prove the fix is the TOKEN, not a swallow — with an empty static map AND no vended token
        // there is genuinely no credential, so normalization must still FAIL LOUD (legacy parity) rather
        // than ship the raw oss:// to BE (silent read corruption). MUTATION: swallowing to the raw path
        // when the token is empty -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);
        Assertions.assertThrows(RuntimeException.class,
                () -> restCtx.resolveStorage(Collections.emptyMap())
                        .normalizeUri("oss://bkt/a/part-0.parquet"));
    }

    @Test
    public void staticMapPathUnaffectedByEmptyToken() throws Exception {
        // WHY: a view resolved with an EMPTY token must fold to the static-map path byte-identically
        // to a token-less view, so non-REST (static-cred) reads are unchanged. MUTATION: an empty token
        // suppressing the static map -> no normalization / throw -> red.
        Assertions.assertEquals("s3://bkt/warehouse/db/t/part-0.parquet",
                ossContext().resolveStorage(Collections.emptyMap())
                        .normalizeUri("oss://bkt/warehouse/db/t/part-0.parquet"));
    }

    // ---- T06 write-sink file type: backendFileType resolves the BE file type via the SAME
    //      LocationPath the legacy IcebergTableSink used (broker-aware), returned as the enum NAME. ----

    @Test
    public void backendFileTypeForOssResolvesToS3ViaLocationPath() throws Exception {
        // WHY: the iceberg write sink must tell BE which file-system family opens the output path. The
        // engine resolves it through LocationPath.getTFileTypeForBE() (same as legacy), so an OSS data
        // location yields FILE_S3 (object store). Returned as the enum NAME (the SPI is Thrift-free).
        // MUTATION: scheme-only default that can't see storage props, or a wrong family -> red.
        Assertions.assertEquals(TFileType.FILE_S3.name(),
                ossContext().resolveStorage(null).backendFileType("oss://bkt/warehouse/db/t/data"));
    }

    @Test
    public void backendFileTypeVendedRestResolvesUnderEmptyStaticMap() {
        // WHY: a REST catalog's static storage map is empty; the vended token resolves the file type the
        // same way it resolves the path. MUTATION: ignoring the token (static-only) throws "no storage
        // properties" -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);
        Assertions.assertEquals(TFileType.FILE_S3.name(),
                restCtx.resolveStorage(ossVendedToken()).backendFileType("oss://bkt/warehouse/db/t/data"));
    }

    // ---- FIX-PERF-06: resolveStorage hoists the (scan-invariant) token->storage-config derivation
    //      to ONCE per scan; every application on the view must stay byte-identical to a
    //      freshly-resolved view with the same token, across all four per-call cases. ----

    @Test
    public void viewVendedMatchesFreshViewAndServesManyUris() {
        // WHY: the scan-scoped view bakes the vended token in once, then normalizes many paths;
        // each application must equal a fresh view's answer (REST empty-static -> vended replaces
        // static), and ONE view must serve multiple files (the whole point of the hoist).
        // MUTATION: dropping the token (static-only) throws; a stale/rebuilt map yielding a different path
        // -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);
        ConnectorStorageView view = restCtx.resolveStorage(ossVendedToken());
        Assertions.assertEquals(
                restCtx.resolveStorage(ossVendedToken()).normalizeUri("oss://bkt/a/f1.parquet"),
                view.normalizeUri("oss://bkt/a/f1.parquet"));
        Assertions.assertEquals("s3://bkt/a/f1.parquet", view.normalizeUri("oss://bkt/a/f1.parquet"));
        // Reuse the SAME view for a second, different path — one derivation, many applications.
        Assertions.assertEquals("s3://bkt/b/f2.parquet", view.normalizeUri("oss://bkt/b/f2.parquet"));
    }

    @Test
    public void viewStaticMapMatchesPerCallUnderEmptyToken() throws Exception {
        // WHY: with a static OSS map and an empty token, the view folds to the static-map path,
        // byte-identical to a fresh view. MUTATION: an empty token suppressing the static map -> red.
        DefaultConnectorContext ctx = ossContext();
        ConnectorStorageView view = ctx.resolveStorage(Collections.emptyMap());
        Assertions.assertEquals(
                ctx.resolveStorage(Collections.emptyMap())
                        .normalizeUri("oss://bkt/warehouse/db/t/part-0.parquet"),
                view.normalizeUri("oss://bkt/warehouse/db/t/part-0.parquet"));
    }

    @Test
    public void viewShortCircuitsNullAndBlankWithoutForcingDerivation() {
        // WHY: same empty-uri short-circuit as the per-call form — a null/blank path returns unchanged
        // and never reaches the fail-loud LocationPath, even on an empty static map + empty token (so a
        // scan that only ever sees blank uris triggers no derivation/throw). MUTATION: NPE / fabricated
        // output / forcing the derivation to throw -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);
        ConnectorStorageView view = restCtx.resolveStorage(Collections.emptyMap());
        Assertions.assertNull(view.normalizeUri(null));
        Assertions.assertEquals("", view.normalizeUri(""));
    }

    @Test
    public void viewFailsLoudOnBadPathLikePerCall() {
        // WHY: fail-loud parity — an empty static map + empty token has no credential, so applying to a
        // real oss:// path must throw (not ship the raw path to BE), exactly like the per-call form.
        // MUTATION: swallowing to the raw path -> red.
        DefaultConnectorContext restCtx = new DefaultConnectorContext("c", 1L);
        ConnectorStorageView view = restCtx.resolveStorage(Collections.emptyMap());
        Assertions.assertThrows(RuntimeException.class, () -> view.normalizeUri("oss://bkt/a/part-0.parquet"));
    }
}
