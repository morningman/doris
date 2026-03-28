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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Lazy file iterator.
 *
 * <p>{@link #hasNext()} and {@link #next()} may throw {@link IOException},
 * which distinguishes this from {@link java.util.Iterator}.
 *
 * <p>References Trino's {@code FileIterator}.
 *
 * <p>This is an independent interface (recommended approach A from design doc),
 * not extending the existing {@code RemoteIterator<T>}. The existing {@code RemoteIterator}
 * will be migrated to this interface in a later phase.
 */
public interface FileIterator {

    /**
     * Returns {@code true} if there are more file entries.
     *
     * @throws IOException if an I/O error occurs while checking
     */
    boolean hasNext() throws IOException;

    /**
     * Returns the next file entry.
     *
     * @throws IOException if an I/O error occurs while fetching
     * @throws NoSuchElementException if no more elements
     */
    FileEntry next() throws IOException;

    /**
     * Returns an empty iterator.
     */
    static FileIterator empty() {
        return new FileIterator() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public FileEntry next() {
                throw new NoSuchElementException("empty iterator");
            }
        };
    }

    /**
     * Creates a FileIterator backed by a pre-fetched list.
     * Useful for wrapping legacy list-based results.
     *
     * @param entries the list of file entries
     * @return a FileIterator over the given list
     */
    static FileIterator fromList(List<FileEntry> entries) {
        Iterator<FileEntry> delegate = entries.iterator();
        return new FileIterator() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public FileEntry next() {
                if (!delegate.hasNext()) {
                    throw new NoSuchElementException();
                }
                return delegate.next();
            }
        };
    }

    /**
     * Convenience method: collects all remaining entries into a List.
     * Use only for small result sets — large listings should consume entries lazily.
     *
     * @return a list of all remaining file entries
     * @throws IOException if an I/O error occurs during iteration
     */
    default List<FileEntry> collectAll() throws IOException {
        List<FileEntry> result = new ArrayList<>();
        while (hasNext()) {
            result.add(next());
        }
        return result;
    }
}
