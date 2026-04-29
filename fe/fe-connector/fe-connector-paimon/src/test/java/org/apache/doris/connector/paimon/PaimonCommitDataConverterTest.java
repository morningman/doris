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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class PaimonCommitDataConverterTest {

    private static byte[] makeFragment(int bucket) throws Exception {
        CommitMessage msg = new CommitMessageImpl(
                BinaryRow.EMPTY_ROW, bucket, 1,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
        return new CommitMessageSerializer().serialize(msg);
    }

    @Test
    void decodeFragmentsRoundTripsBuckets() throws Exception {
        List<CommitMessage> out = PaimonCommitDataConverter.decodeFragments(
                Arrays.asList(makeFragment(2), makeFragment(7)));
        Assertions.assertEquals(2, out.size());
        Assertions.assertEquals(2, out.get(0).bucket());
        Assertions.assertEquals(7, out.get(1).bucket());
    }

    @Test
    void emptyFragmentsRejected() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonCommitDataConverter.decodeFragments(Collections.emptyList()));
        Assertions.assertTrue(ex.getMessage().contains("at least one"));
    }

    @Test
    void nullFragmentsRejected() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonCommitDataConverter.decodeFragments(null));
    }

    @Test
    void zeroLengthFragmentRejected() {
        Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonCommitDataConverter.decodeFragments(
                        Collections.singletonList(new byte[0])));
    }

    @Test
    void malformedFragmentSurfacesIoException() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> PaimonCommitDataConverter.decodeFragments(
                        Collections.singletonList(new byte[]{1, 2, 3, 4, 5})));
        Assertions.assertTrue(ex.getMessage().contains("Failed to deserialize"),
                "expected a deserialization-failure message, got: " + ex.getMessage());
    }
}
