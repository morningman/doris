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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.DorisInputFile;
import org.apache.doris.filesystem.DorisInputStream;
import org.apache.doris.filesystem.DorisOutputFile;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.ObjFileSystem;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;
import org.apache.doris.filesystem.spi.RequestBody;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/* S3-backed FileSystem implementation for the Doris FE filesystem SPI.
 * Does not depend on fe-core, fe-common, or fe-catalog.
 */
public class S3FileSystem extends ObjFileSystem {

    private static final Logger LOG = LogManager.getLogger(S3FileSystem.class);

    // S3 does not have real directories; use a zero-byte marker with trailing slash.
    private static final String DIR_MARKER_SUFFIX = "/";

    public S3FileSystem(S3ObjStorage objStorage) {
        super("S3", objStorage);
    }

    @Override
    protected boolean isNotFoundError(IOException e) {
        return e instanceof java.io.FileNotFoundException;
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        // S3 is flat; create a zero-byte directory marker for compatibility
        String path = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri()
                : location.uri() + DIR_MARKER_SUFFIX;
        objStorage.putObject(path,
                RequestBody.of(InputStream.nullInputStream(), 0));
    }

    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        if (recursive) {
            // Delete all objects under this prefix
            String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                    ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
            deleteRecursive(prefix);
        } else {
            // Per FileSystem contract, recursive=false must fail on a non-empty directory.
            // S3 has no real directories, so probe the prefix for any non-marker child.
            String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                    ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
            String markerKey = stripBucketPrefix(prefix);
            RemoteObjects probe = ((S3ObjStorage) objStorage).listObjects(prefix, null, 2);
            for (RemoteObject obj : probe.getObjectList()) {
                if (!obj.getKey().equals(markerKey)) {
                    throw new IOException("Directory not empty: " + location.uri());
                }
            }
        }
        // Always attempt to delete the exact object too
        try {
            objStorage.deleteObject(location.uri());
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
    }

    private void deleteRecursive(String prefix) throws IOException {
        String continuationToken = null;
        do {
            RemoteObjects batch = objStorage.listObjects(prefix, continuationToken);
            for (RemoteObject obj : batch.getObjectList()) {
                objStorage.deleteObject(reconstructUri(prefix, obj.getKey()));
            }
            continuationToken = batch.isTruncated() ? batch.getContinuationToken() : null;
        } while (continuationToken != null);
    }

    private static String reconstructUri(String prefix, String key) {
        // prefix is like "s3://bucket/path/", key is the full object key
        int schemeEnd = prefix.indexOf("://");
        if (schemeEnd >= 0) {
            String scheme = prefix.substring(0, schemeEnd);
            int slashAfterScheme = prefix.indexOf('/', schemeEnd + 3);
            if (slashAfterScheme >= 0) {
                String bucket = prefix.substring(schemeEnd + 3, slashAfterScheme);
                return scheme + "://" + bucket + "/" + key;
            }
        }
        return prefix + key;
    }

    /** Returns {@code [scheme, bucket, key]} for a {@code scheme://bucket/key} URI. */
    private static String[] parseSchemeBucketKey(String uri) {
        int schemeEnd = uri.indexOf("://");
        if (schemeEnd < 0) {
            return new String[] {"", "", uri};
        }
        String scheme = uri.substring(0, schemeEnd);
        String rest = uri.substring(schemeEnd + 3);
        int slash = rest.indexOf('/');
        if (slash < 0) {
            return new String[] {scheme, rest, ""};
        }
        return new String[] {scheme, rest.substring(0, slash), rest.substring(slash + 1)};
    }

    private static String stripBucketPrefix(String uri) {
        return parseSchemeBucketKey(uri)[2];
    }

    /**
     * S3 has no notion of a real directory; the bare HEAD only finds files.  Fall back to
     * a 1-key prefix listing so that a "directory" that exists solely as a set of children
     * (e.g. an upload tree written by Hive/Spark without explicit markers) still reports
     * existence. Returns true for any non-empty prefix on object stores.
     */
    @Override
    public boolean exists(Location location) throws IOException {
        if (super.exists(location)) {
            return true;
        }
        return prefixHasChildren(location.uri());
    }

    private boolean prefixHasChildren(String uri) throws IOException {
        String prefix = uri.endsWith(DIR_MARKER_SUFFIX) ? uri : uri + DIR_MARKER_SUFFIX;
        RemoteObjects page = ((S3ObjStorage) objStorage).listObjects(prefix, null, 1);
        return !page.getObjectList().isEmpty();
    }

    /**
     * Renames a single object key.
     *
     * <p>This rename is implemented as <em>copy-then-delete</em> and is therefore
     * <strong>not atomic</strong>: a crash between the two calls will leave both copies.
     *
     * <p>Behaviour:
     * <ul>
     *   <li>Throws {@link FileAlreadyExistsException} if {@code dst} already exists.</li>
     *   <li>Refuses to rename a "directory" (a prefix with children but no key) and
     *       throws {@link IOException}; callers must use
     *       {@link #renameDirectory(Location, Location, Runnable)} for that case.</li>
     * </ul>
     */
    @Override
    public void rename(Location src, Location dst) throws IOException {
        // 1) Reject if destination already exists.
        boolean dstExists;
        try {
            objStorage.headObject(dst.uri());
            dstExists = true;
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
            dstExists = false;
        }
        if (dstExists) {
            throw new FileAlreadyExistsException(dst.uri());
        }
        // 2) Refuse directory-prefix renames so children are not silently orphaned.
        boolean srcIsKey;
        try {
            objStorage.headObject(src.uri());
            srcIsKey = true;
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
            srcIsKey = false;
        }
        if (!srcIsKey && prefixHasChildren(src.uri())) {
            throw new IOException("rename: source is a directory prefix with children; "
                    + "use renameDirectory(): " + src.uri());
        }
        objStorage.copyObject(src.uri(), dst.uri());
        objStorage.deleteObject(src.uri());
    }

    /**
     * Atomically-named (but non-atomic in implementation) directory move for object stores.
     *
     * <p>S3 has no real directories, so this method:
     * <ol>
     *   <li>Lists every key under {@code src + "/"} (paginated).</li>
     *   <li>If the listing is empty, runs {@code whenSrcNotExists} and returns.</li>
     *   <li>Verifies that {@code dst + "/"} has no objects yet; otherwise throws
     *       {@link FileAlreadyExistsException} <em>before</em> any copy.</li>
     *   <li>Copies each child to the matching destination key.</li>
     *   <li>Batch-deletes the source keys via
     *       {@link S3ObjStorage#deleteObjectsByKeys(String, java.util.List)}.</li>
     * </ol>
     *
     * <p>The default implementation in {@link org.apache.doris.filesystem.FileSystem} is unsafe on
     * S3 because it relies on {@code exists(src)} (which is false for marker-less directories)
     * and {@link #rename(Location, Location)} (which only handles a single key).
     */
    @Override
    public void renameDirectory(Location src, Location dst, Runnable whenSrcNotExists)
            throws IOException {
        String srcUri = src.uri();
        String dstUri = dst.uri();
        String srcPrefix = srcUri.endsWith(DIR_MARKER_SUFFIX) ? srcUri : srcUri + DIR_MARKER_SUFFIX;
        String dstPrefix = dstUri.endsWith(DIR_MARKER_SUFFIX) ? dstUri : dstUri + DIR_MARKER_SUFFIX;

        S3ObjStorage s3 = (S3ObjStorage) objStorage;

        // 1. Collect all source keys (paginated).
        List<String> srcKeys = new ArrayList<>();
        String continuation = null;
        do {
            RemoteObjects page = s3.listObjects(srcPrefix, continuation);
            for (RemoteObject obj : page.getObjectList()) {
                srcKeys.add(obj.getKey());
            }
            continuation = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuation != null);

        if (srcKeys.isEmpty()) {
            whenSrcNotExists.run();
            return;
        }

        // 2. Refuse to clobber a non-empty destination prefix.
        RemoteObjects dstProbe = s3.listObjects(dstPrefix, null, 1);
        if (!dstProbe.getObjectList().isEmpty()) {
            throw new FileAlreadyExistsException(dstUri);
        }

        // 3. Copy every key under src to the matching destination key.
        String srcKeyPrefix = stripBucketPrefix(srcPrefix);
        String dstKeyPrefix = stripBucketPrefix(dstPrefix);
        String[] srcParts = parseSchemeBucketKey(srcUri);
        String scheme = srcParts[0];
        String bucket = srcParts[1];
        for (String key : srcKeys) {
            String relative = key.startsWith(srcKeyPrefix)
                    ? key.substring(srcKeyPrefix.length()) : key;
            String dstKey = dstKeyPrefix + relative;
            objStorage.copyObject(scheme + "://" + bucket + "/" + key,
                    scheme + "://" + bucket + "/" + dstKey);
        }

        // 4. Batch-delete the original keys.
        s3.deleteObjectsByKeys(bucket, srcKeys);
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        // S3 list-objects is prefix-based, not directory-based: listing
        // "s3://bucket/foo" would also return objects under "foo_bar/" because
        // they share the same string prefix. The FileSystem.list contract
        // specifies a directory, so enforce a trailing '/' to constrain the
        // prefix to a true directory boundary.
        String uri = location.uri();
        if (!uri.endsWith(DIR_MARKER_SUFFIX)) {
            uri = uri + DIR_MARKER_SUFFIX;
        }
        return new S3FileIterator(uri);
    }

    /**
     * Returns the non-directory entries directly under {@code location} (non-recursive),
     * matching the {@link org.apache.doris.filesystem.FileSystem#listFiles} contract.
     *
     * <p>Unlike {@link #list(Location)} (which is a flat recursive listing on object stores),
     * this override uses S3 delimiter-mode listing so that only objects in this single
     * "directory level" are returned. Sub-directories (S3 {@code CommonPrefixes}) are
     * intentionally <em>not</em> exposed here — callers needing them must use the iterator
     * from {@link #list(Location)} or a separate listing method. The synthetic directory
     * marker created by {@link #mkdirs(Location)} (a zero-byte object whose key ends in
     * {@code "/"}) is filtered out of the result.
     *
     * <p>Glob expansion (paths containing {@code *}, {@code ?}, etc.) still flows through
     * {@link #globListWithLimit(Location, String, long, long)} which performs its own
     * recursive prefix scan with PathMatcher filtering.
     */
    @Override
    public List<FileEntry> listFiles(Location location) throws IOException {
        // BrokerLoadPendingTask passes paths with glob characters (e.g. _*).
        // S3 list-objects does not expand globs; reuse globListWithLimit for glob paths.
        if (containsGlob(location.toString())) {
            GlobListing listing = globListWithLimit(location, null, -1L, -1L);
            return listing.getFiles();
        }
        String uri = location.uri();
        String prefix = uri.endsWith(DIR_MARKER_SUFFIX) ? uri : uri + DIR_MARKER_SUFFIX;
        String prefixKey = stripBucketPrefix(prefix);

        S3ObjStorage s3 = (S3ObjStorage) objStorage;
        List<FileEntry> result = new ArrayList<>();
        String continuation = null;
        do {
            RemoteObjects page = s3.listObjectsNonRecursive(prefix, continuation);
            for (RemoteObject obj : page.getObjectList()) {
                // Skip the directory marker placeholder: key equals the listing prefix
                // or otherwise ends with "/".
                if (obj.getKey().equals(prefixKey)
                        || obj.getKey().endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                result.add(new FileEntry(
                        Location.of(reconstructUri(prefix, obj.getKey())),
                        obj.getSize(), false, obj.getModificationTime(), List.of()));
            }
            continuation = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuation != null);
        return result;
    }
    // TODO(audit#6): listDirectories may also be improved to surface S3 CommonPrefixes;
    // skipped here to keep the scope of this change small. See plan-doc/s3.md.

    private static boolean containsGlob(String path) {
        return path.contains("*") || path.contains("?") || path.contains("[") || path.contains("{");
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return new S3InputFile(location);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        return new S3OutputFile(location);
    }

    /** Lazy-loading, paginating FileIterator over S3 list results. */
    private class S3FileIterator implements FileIterator {
        private final String prefix;
        private String continuationToken;
        private List<FileEntry> buffer = new ArrayList<>();
        private int bufferIdx = 0;
        private boolean done = false;

        S3FileIterator(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public boolean hasNext() throws IOException {
            if (bufferIdx < buffer.size()) {
                return true;
            }
            if (done) {
                return false;
            }
            fetchNextPage();
            return bufferIdx < buffer.size();
        }

        private void fetchNextPage() throws IOException {
            RemoteObjects page = objStorage.listObjects(prefix, continuationToken);
            buffer = new ArrayList<>();
            bufferIdx = 0;
            for (RemoteObject obj : page.getObjectList()) {
                // Skip directory-marker placeholders (e.g. created by mkdirs):
                // those are zero-byte objects whose key ends with "/" or equals the
                // listing prefix's bare key.  Emitting them as FileEntry causes
                // downstream readers to attempt opening a non-existent file.
                if (obj.getKey().endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                Location loc = Location.of(reconstructUri(prefix, obj.getKey()));
                buffer.add(new FileEntry(loc, obj.getSize(), false, obj.getModificationTime(), List.of()));
            }
            if (page.isTruncated()) {
                continuationToken = page.getContinuationToken();
            } else {
                done = true;
            }
        }

        @Override
        public FileEntry next() throws IOException {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return buffer.get(bufferIdx++);
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    /** S3-backed DorisInputFile. */
    private class S3InputFile implements DorisInputFile {
        private final Location location;

        S3InputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public long length() throws IOException {
            return objStorage.headObject(location.uri()).getSize();
        }

        @Override
        public boolean exists() throws IOException {
            try {
                objStorage.headObject(location.uri());
                return true;
            } catch (IOException e) {
                if (isNotFoundError(e)) {
                    return false;
                }
                throw e;
            }
        }

        @Override
        public long lastModifiedTime() throws IOException {
            return ((S3ObjStorage) objStorage).headObjectLastModified(location.uri());
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            long fileLength = length();
            return new S3SeekableInputStream(location.uri(), (S3ObjStorage) objStorage, fileLength);
        }
    }

    /**
     * Seekable input stream for S3 objects.
     * Uses HTTP Range requests to seek without downloading the entire object.
     */
    private static class S3SeekableInputStream extends DorisInputStream {
        private final String remotePath;
        private final S3ObjStorage objStorage;
        private final long fileLength;
        private long position;
        private InputStream current;
        private boolean closed;

        S3SeekableInputStream(String remotePath, S3ObjStorage objStorage, long fileLength) {
            this.remotePath = remotePath;
            this.objStorage = objStorage;
            this.fileLength = fileLength;
        }

        private void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed: " + remotePath);
            }
        }

        /** Opens a range-based GET stream starting at {@link #position}. */
        private void openStream() throws IOException {
            if (current != null) {
                current.close();
                current = null;
            }
            current = objStorage.openInputStreamAt(remotePath, position);
        }

        @Override
        public long getPos() throws IOException {
            checkOpen();
            return position;
        }

        @Override
        public void seek(long pos) throws IOException {
            checkOpen();
            if (pos < 0 || pos > fileLength) {
                throw new IOException("Seek position out of range [0, " + fileLength + "]: " + pos);
            }
            if (pos == position) {
                return;
            }
            // Close the current stream; a new range request will be issued on next read.
            if (current != null) {
                current.close();
                current = null;
            }
            position = pos;
        }

        @Override
        public int read() throws IOException {
            checkOpen();
            if (position >= fileLength) {
                return -1;
            }
            ensureOpen();
            int b = current.read();
            if (b >= 0) {
                position++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            checkOpen();
            if (position >= fileLength) {
                return -1;
            }
            ensureOpen();
            int n = current.read(b, off, len);
            if (n > 0) {
                position += n;
            }
            return n;
        }

        private void ensureOpen() throws IOException {
            if (current == null) {
                openStream();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (current != null) {
                current.close();
                current = null;
            }
        }
    }

    /** S3-backed DorisOutputFile. */
    private class S3OutputFile implements DorisOutputFile {
        private final Location location;

        S3OutputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public OutputStream create() throws IOException {
            // Per DorisOutputFile contract, create() must fail if the target object
            // already exists. Probe with headObject and translate not-found into a
            // signal that we may proceed with the PUT.
            try {
                objStorage.headObject(location.uri());
            } catch (IOException e) {
                if (isNotFoundError(e)) {
                    return createOrOverwrite();
                }
                throw e;
            }
            throw new FileAlreadyExistsException(location.uri());
        }

        @Override
        public OutputStream createOrOverwrite() throws IOException {
            // Use a buffered in-memory stream; flush triggers PutObject on close
            return new S3OutputStream(location.uri(), (S3ObjStorage) objStorage);
        }
    }

    /**
     * Returns the longest key-prefix of {@code globPattern} that contains no glob metacharacters
     * ({@code * ? [ { \}).  Used as the {@code prefix} parameter for S3 {@code ListObjectsV2}.
     */
    static String longestNonGlobPrefix(String globPattern) {
        int earliest = globPattern.length();
        for (char c : new char[]{'*', '?', '[', '{', '\\'}) {
            int idx = globPattern.indexOf(c);
            if (idx >= 0 && idx < earliest) {
                earliest = idx;
            }
        }
        return globPattern.substring(0, earliest);
    }

    /**
     * Expands {@code {N..M}} numeric range syntax in a glob pattern to the equivalent
     * comma-separated alternation {@code {N,N+1,...,M}} that Java's PathMatcher understands.
     * Supports simple non-negative ranges like {@code {1..3}}, reverse ranges like {@code {3..1}},
     * and mixed comma-separated patterns like {@code {1..2,3,1..3}} or {@code {Refrain,1..3}}.
     * For simple (no-comma) brace groups, only non-negative ranges are expanded;
     * negative-start ranges like {@code {-1..1}} are left unchanged.
     * Duplicate values are removed. For example:
     * <ul>
     *   <li>{@code data_{1..3}.csv} → {@code data_{1,2,3}.csv}</li>
     *   <li>{@code data_{1..2,3,1..3}.csv} → {@code data_{1,2,3}.csv}</li>
     *   <li>{@code data_{Refrain,1..3}.csv} → {@code data_{Refrain,1,2,3}.csv}</li>
     *   <li>{@code data_{-1..1}.csv} → unchanged (no expansion)</li>
     * </ul>
     */
    private static String expandNumericRanges(String pattern) {
        java.util.regex.Pattern rangeSegment = java.util.regex.Pattern.compile(
                "(-?\\d+)\\.\\.(-?\\d+)");
        java.util.regex.Pattern simpleRange = java.util.regex.Pattern.compile(
                "\\{(\\d+)\\.\\.(\\d+)\\}");
        // Match any brace group that contains at least one N..M range
        java.util.regex.Pattern braceGroup = java.util.regex.Pattern.compile(
                "\\{([^}]*\\d+\\.\\.\\d+[^}]*)\\}");
        java.util.regex.Matcher m = braceGroup.matcher(pattern);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String content = m.group(1);
            boolean isMixed = content.contains(",");
            if (!isMixed) {
                // Simple brace group (no comma): only expand non-negative ranges
                java.util.regex.Matcher sm = simpleRange.matcher(m.group(0));
                if (!sm.matches()) {
                    // Not a simple non-negative range (e.g., {-1..1}) — leave unchanged
                    continue;
                }
            }
            String[] segments = content.split(",", -1);
            java.util.LinkedHashSet<String> values = new java.util.LinkedHashSet<>();
            for (String seg : segments) {
                java.util.regex.Matcher rm = rangeSegment.matcher(seg.trim());
                if (rm.matches()) {
                    int from = Integer.parseInt(rm.group(1));
                    int to = Integer.parseInt(rm.group(2));
                    int step = from <= to ? 1 : -1;
                    for (int i = from; step > 0 ? i <= to : i >= to; i += step) {
                        values.add(String.valueOf(i));
                    }
                } else {
                    values.add(seg.trim());
                }
            }
            StringBuilder expansion = new StringBuilder("{");
            expansion.append(String.join(",", values));
            expansion.append('}');
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(expansion.toString()));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        // Parse s3://bucket/keyPattern from the Location URI
        String uri = path.uri();
        int schemeEnd = uri.indexOf("://");
        String bucketAndKey = schemeEnd >= 0 ? uri.substring(schemeEnd + 3) : uri;
        int firstSlash = bucketAndKey.indexOf('/');
        String bucket = firstSlash >= 0 ? bucketAndKey.substring(0, firstSlash) : bucketAndKey;
        String keyPattern = firstSlash >= 0 ? bucketAndKey.substring(firstSlash + 1) : "";

        String expandedKeyPattern = expandNumericRanges(keyPattern);
        java.nio.file.Path pathPattern = Paths.get(expandedKeyPattern);
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern);
        String listPrefix = longestNonGlobPrefix(expandedKeyPattern);

        S3ObjStorage s3 = (S3ObjStorage) objStorage;
        ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(listPrefix);
        if (startAfter != null && !startAfter.isEmpty()) {
            reqBuilder.startAfter(startAfter);
        }
        ListObjectsV2Request request = reqBuilder.build();

        List<FileEntry> files = new ArrayList<>();
        long totalSize = 0L;
        boolean reachLimit = false;
        // nextMatchAfterLimit: the first matching key found after the page limit was reached.
        // Empty string means no such key was found yet (scanning still in progress or no more keys).
        String nextMatchAfterLimit = "";
        String lastMatchedKey = "";
        boolean isTruncated;

        try {
            do {
                ListObjectsV2Response response = s3.getClient().listObjectsV2(request);
                for (S3Object obj : response.contents()) {
                    if (reachLimit) {
                        // After hitting limit: find the first matching key so callers know more data exists.
                        if (nextMatchAfterLimit.isEmpty()
                                && matcher.matches(Paths.get(obj.key()))) {
                            nextMatchAfterLimit = obj.key();
                        }
                        continue;
                    }

                    if (!matcher.matches(Paths.get(obj.key()))) {
                        continue;
                    }

                    files.add(new FileEntry(
                            Location.of("s3://" + bucket + "/" + obj.key()),
                            obj.size(),
                            false,
                            obj.lastModified() != null ? obj.lastModified().toEpochMilli() : 0L,
                            null));
                    totalSize += obj.size();
                    lastMatchedKey = obj.key();

                    if ((maxFiles > 0 && files.size() >= maxFiles)
                            || (maxBytes > 0 && totalSize >= maxBytes)) {
                        reachLimit = true;
                    }
                }

                isTruncated = response.isTruncated();
                if (isTruncated) {
                    request = request.toBuilder()
                            .continuationToken(response.nextContinuationToken())
                            .build();
                }
                // Continue paginating after limit until we find the next matching key,
                // so callers can use it as a pagination cursor.
            } while (isTruncated && (!reachLimit || nextMatchAfterLimit.isEmpty()));
        } catch (NoSuchKeyException e) {
            LOG.info("NoSuchKey when listing s3://{}/{}, treating as empty", bucket, listPrefix);
            return new GlobListing(List.of(), bucket, listPrefix, "");
        } catch (Exception e) {
            throw new IOException("Failed to list S3 objects at " + uri + ": " + e.getMessage(), e);
        }

        // maxFile is the next matching key after the returned page (if found), or the last returned key.
        String maxFile = nextMatchAfterLimit.isEmpty() ? lastMatchedKey : nextMatchAfterLimit;
        return new GlobListing(files, bucket, listPrefix, maxFile);
    }
}
