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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.DorisConnectorException;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Plugin-side conversion utilities used by the Paimon WriteOps path.
 *
 * <p>Bridges the BE → FE write protocol (paimon-native serialized
 * {@link CommitMessage} byte buffers) into in-memory
 * {@link CommitMessage} instances ready to be passed into
 * {@link org.apache.paimon.table.sink.BatchTableCommit#commit(List)}.</p>
 *
 * <p>The wire format is paimon's own
 * {@link CommitMessageSerializer} output at
 * {@link CommitMessageSerializer#CURRENT_VERSION}; both sides link
 * against the same paimon SDK version so version negotiation is not
 * required at this layer. Each fragment must encode exactly one
 * {@link CommitMessage}; lists are split into multiple fragments by the
 * BE writer.</p>
 *
 * <p>UPSERT row-ops on primary-key tables are encoded inside the
 * paimon {@link CommitMessage} payload itself (insert vs delete records
 * are distinguished by paimon's row-kind/{@code _DORIS_DELETE_SIGN_}
 * machinery in the BE writer), so this converter does not need to
 * dispatch on intent type.</p>
 */
final class PaimonCommitDataConverter {

    private PaimonCommitDataConverter() {
    }

    /**
     * Decodes a non-empty collection of paimon-serialized
     * {@link CommitMessage} fragments into the corresponding in-memory
     * objects, preserving the iteration order of {@code fragments}.
     *
     * <p>Empty / null inputs are rejected loudly because a successful
     * write that produced zero commit messages would otherwise silently
     * commit nothing on a paimon transaction that does not tolerate it
     * (paimon's {@code BatchTableCommit} treats an empty commit list as
     * a valid no-op, masking lost-fragment bugs).</p>
     *
     * @throws DorisConnectorException if the fragment list is empty,
     *         contains a null/empty entry, or any fragment fails to
     *         deserialize.
     */
    static List<CommitMessage> decodeFragments(Collection<byte[]> fragments) {
        if (fragments == null || fragments.isEmpty()) {
            throw new DorisConnectorException(
                    "Paimon commit requires at least one CommitMessage fragment, got none");
        }
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        int version = CommitMessageSerializer.CURRENT_VERSION;
        List<CommitMessage> messages = new ArrayList<>(fragments.size());
        int index = 0;
        for (byte[] payload : fragments) {
            if (payload == null || payload.length == 0) {
                throw new DorisConnectorException(
                        "Paimon commit fragment #" + index + " is null or empty");
            }
            try {
                messages.add(serializer.deserialize(version, payload));
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Failed to deserialize paimon CommitMessage fragment #" + index, e);
            }
            index++;
        }
        return messages;
    }
}
