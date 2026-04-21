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

package org.apache.doris.filesystem.azure;

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Azure Blob Storage-backed {@link org.apache.doris.filesystem.FileSystem} implementation.
 *
 * <p>Does not depend on fe-core, fe-common, or fe-catalog.
 * Azure does not support atomic directory renames; {@link #rename} is limited to single blobs.
 */
public class AzureFileSystem extends ObjFileSystem {

    private static final Logger LOG = LogManager.getLogger(AzureFileSystem.class);

    private static final String DIR_MARKER_SUFFIX = "/";

    public AzureFileSystem(AzureObjStorage objStorage) {
        super("AZURE", objStorage);
    }

    @Override
    protected boolean isNotFoundError(IOException e) {
        return e instanceof java.io.FileNotFoundException
                || (e.getMessage() != null && e.getMessage().contains("404"));
    }

    @Override
    public void mkdirs(Location location) throws IOException {
        // Azure is a flat namespace; create a zero-byte directory marker for compatibility.
        String path = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri()
                : location.uri() + DIR_MARKER_SUFFIX;
        objStorage.putObject(path, RequestBody.of(InputStream.nullInputStream(), 0));
    }

    /**
     * Deletes a blob or virtual directory.
     *
     * <p>When {@code recursive=true}, paginates the listing under {@code <uri>/} and deletes
     * each entry — including the directory marker blob {@code <uri>/} itself when present.
     * The literal key {@code <uri>} (without trailing slash) is intentionally NOT deleted by
     * the recursive branch: in Azure's flat namespace, deleting a directory must not also
     * remove a sibling blob whose name happens to equal the directory path.
     *
     * <p>When {@code recursive=false}, first checks whether any child blobs exist under
     * {@code <uri>/}; if so, throws {@link IOException} ("Directory not empty"). Otherwise
     * issues a best-effort {@code deleteObject(uri)} on the literal key. A 404 (target does
     * not exist) is swallowed so that deleting a missing target is idempotent — matching
     * the contract honored by {@code S3FileSystem}.
     */
    @Override
    public void delete(Location location, boolean recursive) throws IOException {
        String prefix = location.uri().endsWith(DIR_MARKER_SUFFIX)
                ? location.uri() : location.uri() + DIR_MARKER_SUFFIX;
        if (recursive) {
            deleteRecursive(prefix);
            return;
        }
        if (hasChildUnder(prefix)) {
            throw new IOException("Directory not empty: " + location);
        }
        try {
            objStorage.deleteObject(location.uri());
        } catch (IOException e) {
            if (!isNotFoundError(e)) {
                throw e;
            }
        }
    }

    /**
     * Returns true if {@code prefix} (a URI ending in {@code /}) has at least one
     * blob whose name is strictly longer than the prefix's key portion (i.e. a child).
     * The directory marker blob whose key equals the prefix's key portion is not
     * considered a child.
     */
    private boolean hasChildUnder(String prefix) throws IOException {
        String prefixKey = AzureUri.parse(prefix).key();
        RemoteObjects firstPage = objStorage.listObjects(prefix, null);
        for (RemoteObject obj : firstPage.getObjectList()) {
            if (obj.getKey().length() > prefixKey.length()) {
                return true;
            }
        }
        return false;
    }

    private void deleteRecursive(String prefix) throws IOException {
        String continuationToken = null;
        do {
            RemoteObjects batch = objStorage.listObjects(prefix, continuationToken);
            for (RemoteObject obj : batch.getObjectList()) {
                objStorage.deleteObject(rebuildUri(prefix, obj.getKey()));
            }
            continuationToken = batch.isTruncated() ? batch.getContinuationToken() : null;
        } while (continuationToken != null);
    }

    /**
     * Azure Blob Storage does not support atomic directory renames.
     * Single-blob renames are supported via copy-then-delete.
     *
     * @throws IOException if the source appears to be a directory prefix
     */
    @Override
    public void rename(Location src, Location dst) throws IOException {
        if (src.uri().endsWith(DIR_MARKER_SUFFIX)) {
            throw new IOException(
                    "Renaming directories is not supported in Azure Blob Storage.");
        }
        objStorage.copyObject(src.uri(), dst.uri());
        objStorage.deleteObject(src.uri());
    }

    @Override
    public FileIterator list(Location location) throws IOException {
        // Azure list-blobs is prefix-based, not directory-based: listing
        // "wasbs://c@a.host/foo" would also return blobs under "foo_bar/"
        // because they share the same string prefix. The FileSystem.list
        // contract specifies a directory, so enforce a trailing '/' to
        // constrain the prefix to a true directory boundary.
        String uri = location.uri();
        if (!uri.endsWith(DIR_MARKER_SUFFIX)) {
            uri = uri + DIR_MARKER_SUFFIX;
        }
        return new AzureFileIterator(uri);
    }

    @Override
    public DorisInputFile newInputFile(Location location) throws IOException {
        return new AzureInputFile(location);
    }

    @Override
    public DorisOutputFile newOutputFile(Location location) throws IOException {
        return new AzureOutputFile(location);
    }

    private static String rebuildUri(String prefix, String key) {
        // prefix is like "wasbs://container@account.blob.core.windows.net/path/",
        // key is the full blob name; reconstruct the full URI for the key.
        int schemeEnd = prefix.indexOf("://");
        if (schemeEnd < 0) {
            return prefix + key;
        }
        String scheme = prefix.substring(0, schemeEnd);
        String withoutScheme = prefix.substring(schemeEnd + 3);
        // For wasb/abfs: "container@account.host/path/"
        int firstSlash = withoutScheme.indexOf('/');
        if (firstSlash < 0) {
            return prefix + key;
        }
        String authority = withoutScheme.substring(0, firstSlash);
        return scheme + "://" + authority + "/" + key;
    }

    @Override
    public GlobListing globListWithLimit(Location path, String startAfter, long maxBytes,
            long maxFiles) throws IOException {
        String uri = path.uri();
        AzureUri parsed = AzureUri.parse(uri);
        String container = parsed.container();
        String keyPattern = parsed.key();
        // base = uri with the key portion stripped, preserving the original scheme/host syntax.
        String base = uri.substring(0, uri.length() - keyPattern.length());

        Pattern matcher = Pattern.compile(globToRegex(keyPattern));
        String listKeyPrefix = longestNonGlobPrefix(keyPattern);
        String listPrefixUri = base + listKeyPrefix;

        List<FileEntry> files = new ArrayList<>();
        long totalSize = 0L;
        String maxFile = "";
        String continuationToken = null;
        boolean reachLimit = false;

        outer:
        do {
            RemoteObjects page = objStorage.listObjects(listPrefixUri, continuationToken);
            for (RemoteObject obj : page.getObjectList()) {
                String key = obj.getKey();
                if (key.endsWith(DIR_MARKER_SUFFIX)) {
                    continue;
                }
                if (startAfter != null && !startAfter.isEmpty() && key.compareTo(startAfter) <= 0) {
                    continue;
                }
                if (!matcher.matcher(key).matches()) {
                    continue;
                }
                if ((maxFiles > 0 && files.size() >= maxFiles)
                        || (maxBytes > 0 && totalSize >= maxBytes)) {
                    maxFile = key;
                    reachLimit = true;
                    break outer;
                }
                files.add(new FileEntry(
                        Location.of(base + key),
                        obj.getSize(),
                        false,
                        obj.getModificationTime(),
                        null));
                totalSize += obj.getSize();
                maxFile = key;
            }
            continuationToken = page.isTruncated() ? page.getContinuationToken() : null;
        } while (continuationToken != null);

        if (!reachLimit && files.isEmpty()) {
            maxFile = "";
        }
        return new GlobListing(files, container, listKeyPrefix, maxFile);
    }

    /**
     * Returns true iff {@code path} contains a glob metacharacter.  Only {@code *} and
     * {@code ?} are recognised — literal {@code [} / {@code {} are valid characters in
     * Azure blob keys and must not be auto-routed through {@link #globListWithLimit}.
     */
    static boolean containsGlob(String path) {
        return path.indexOf('*') >= 0 || path.indexOf('?') >= 0;
    }

    /**
     * Returns the longest key-prefix of {@code globPattern} that contains no glob
     * metacharacters ({@code * ? [ { \}). Used as the {@code prefix} parameter for the
     * Azure list-blobs call.
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
     * Translates a Java NIO glob pattern to a regex matched against raw blob keys.
     * Mirrors the JDK glob → regex conversion but operates on plain strings, so it
     * tolerates characters that the host OS path syntax would refuse.  Supports:
     * <ul>
     *   <li>{@code *} → any run of non-{@code /} characters;</li>
     *   <li>{@code **} → any run of characters including {@code /};</li>
     *   <li>{@code ?} → any single non-{@code /} character;</li>
     *   <li>{@code [...]} character class (with {@code !} for negation);</li>
     *   <li>{@code {a,b,c}} alternation;</li>
     *   <li>{@code \X} escapes the next character literally.</li>
     * </ul>
     */
    static String globToRegex(String glob) {
        StringBuilder sb = new StringBuilder("^");
        boolean inClass = false;
        boolean inGroup = false;
        int i = 0;
        while (i < glob.length()) {
            char c = glob.charAt(i);
            if (c == '\\') {
                if (i + 1 < glob.length()) {
                    sb.append(Pattern.quote(String.valueOf(glob.charAt(i + 1))));
                    i += 2;
                } else {
                    sb.append("\\\\");
                    i++;
                }
                continue;
            }
            if (inClass) {
                if (c == ']') {
                    inClass = false;
                    sb.append(']');
                } else if (c == '\\' || c == '[') {
                    sb.append('\\').append(c);
                } else {
                    sb.append(c);
                }
                i++;
                continue;
            }
            switch (c) {
                case '*':
                    if (i + 1 < glob.length() && glob.charAt(i + 1) == '*') {
                        sb.append(".*");
                        i += 2;
                    } else {
                        sb.append("[^/]*");
                        i++;
                    }
                    break;
                case '?':
                    sb.append("[^/]");
                    i++;
                    break;
                case '[':
                    inClass = true;
                    sb.append('[');
                    i++;
                    if (i < glob.length() && glob.charAt(i) == '!') {
                        sb.append('^');
                        i++;
                    }
                    break;
                case '{':
                    inGroup = true;
                    sb.append("(?:");
                    i++;
                    break;
                case '}':
                    inGroup = false;
                    sb.append(')');
                    i++;
                    break;
                case ',':
                    if (inGroup) {
                        sb.append('|');
                    } else {
                        sb.append(',');
                    }
                    i++;
                    break;
                default:
                    if ("\\.^$|+()".indexOf(c) >= 0) {
                        sb.append('\\').append(c);
                    } else {
                        sb.append(c);
                    }
                    i++;
                    break;
            }
        }
        sb.append('$');
        return sb.toString();
    }

    /** Lazy-paginating FileIterator over Azure list results. */
    private class AzureFileIterator implements FileIterator {
        private final String prefix;
        private String continuationToken;
        private List<FileEntry> buffer = new ArrayList<>();
        private int bufferIdx = 0;
        private boolean done = false;

        AzureFileIterator(String prefix) {
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
                Location loc = Location.of(rebuildUri(prefix, obj.getKey()));
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
            return buffer.get(bufferIdx++);
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    /** Azure-backed DorisInputFile. */
    private class AzureInputFile implements DorisInputFile {
        private final Location location;

        AzureInputFile(Location location) {
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
            return ((AzureObjStorage) objStorage).headObjectLastModified(location.uri());
        }

        @Override
        public DorisInputStream newStream() throws IOException {
            long fileLength = length();
            return new AzureSeekableInputStream(location.uri(), (AzureObjStorage) objStorage, fileLength);
        }
    }

    /**
     * Seekable input stream for Azure blobs.
     * Uses Azure HTTP Range requests to seek without re-downloading the entire blob.
     */
    private static class AzureSeekableInputStream extends DorisInputStream {
        private final String remotePath;
        private final AzureObjStorage objStorage;
        private final long fileLength;
        private long position;
        private java.io.InputStream current;
        private boolean closed;

        AzureSeekableInputStream(String remotePath, AzureObjStorage objStorage, long fileLength) {
            this.remotePath = remotePath;
            this.objStorage = objStorage;
            this.fileLength = fileLength;
        }

        private void checkOpen() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed: " + remotePath);
            }
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
                current = objStorage.openInputStreamAt(remotePath, position);
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

    /** Azure-backed DorisOutputFile (buffered in memory; uploads on close). */
    private class AzureOutputFile implements DorisOutputFile {
        private final Location location;

        AzureOutputFile(Location location) {
            this.location = location;
        }

        @Override
        public Location location() {
            return location;
        }

        @Override
        public OutputStream create() throws IOException {
            return createOrOverwrite();
        }

        @Override
        public OutputStream createOrOverwrite() throws IOException {
            return new AzureOutputStream(location.uri(), (AzureObjStorage) objStorage);
        }
    }

    /** OutputStream that buffers writes in memory and uploads to Azure Blob on close. */
    private static class AzureOutputStream extends OutputStream {
        private final String remotePath;
        private final AzureObjStorage storage;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private boolean closed = false;

        AzureOutputStream(String remotePath, AzureObjStorage storage) {
            this.remotePath = remotePath;
            this.storage = storage;
        }

        @Override
        public void write(int b) throws IOException {
            checkNotClosed();
            buffer.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkNotClosed();
            buffer.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            byte[] data = buffer.toByteArray();
            storage.putObject(remotePath, RequestBody.of(new ByteArrayInputStream(data), data.length));
        }

        private void checkNotClosed() throws IOException {
            if (closed) {
                throw new IOException("Stream already closed: " + remotePath);
            }
        }
    }
}
