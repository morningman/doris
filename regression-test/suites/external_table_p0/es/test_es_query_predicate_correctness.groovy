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

// Regression tests for ES connector predicate pushdown correctness.
// Covers fixes:
//   P0-4: REGEXP must generate ES 'regexp' query, not 'wildcard'
//   P1-3: IS NULL must generate 'must_not exists', not 'term null'

suite("test_es_query_predicate_correctness", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists es7_pred_test;"""
        sql """drop catalog if exists es8_pred_test;"""

        sql """create catalog if not exists es7_pred_test properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );"""

        sql """create catalog if not exists es8_pred_test properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );"""

        // ======================================================================

        def testEsPredicate = { String catalogName ->
            sql """switch ${catalogName}"""

            // ==================================================================
            // P0-4: REGEXP predicate pushdown
            // Before fix: SQL REGEXP was translated to ES 'wildcard' query type,
            // which treats brackets literally. 'string[12]' as wildcard does NOT
            // match 'string1' or 'string2'.
            // After fix: REGEXP correctly generates ES 'regexp' query type, where
            // 'string[12]' is a proper regex matching 'string1' and 'string2'.
            // ==================================================================

            // REGEXP with character class [12] should match string1 and string2
            // The test1 field contains: 'string1', 'string2', 'string3'
            // With correct 'regexp' query: matches string1, string2 (2 rows)
            // With buggy 'wildcard' query: matches nothing (0 rows)
            def regexpResult = sql """select test1 from test1 where test1 regexp 'string[12]' order by test1;"""
            assertTrue(regexpResult.size() >= 2,
                "${catalogName}: REGEXP 'string[12]' should match at least 2 rows (string1, string2), got ${regexpResult.size()}")
            assertTrue(regexpResult.collect { it[0] }.contains("string1"),
                "${catalogName}: REGEXP 'string[12]' should match 'string1'")
            assertTrue(regexpResult.collect { it[0] }.contains("string2"),
                "${catalogName}: REGEXP 'string[12]' should match 'string2'")
            assertTrue(!regexpResult.collect { it[0] }.contains("string3"),
                "${catalogName}: REGEXP 'string[12]' should NOT match 'string3'")

            // REGEXP with dot-star should match all strings starting with 'string'
            def regexpAll = sql """select test1 from test1 where test1 regexp 'string.*' order by test1;"""
            assertTrue(regexpAll.size() >= 3,
                "${catalogName}: REGEXP 'string.*' should match at least 3 rows, got ${regexpAll.size()}")

            // REGEXP with alternation
            def regexpAlt = sql """select test1 from test1 where test1 regexp 'string1|string3' order by test1;"""
            assertTrue(regexpAlt.size() >= 2,
                "${catalogName}: REGEXP alternation should match at least 2 rows, got ${regexpAlt.size()}")
            assertTrue(regexpAlt.collect { it[0] }.contains("string1"),
                "${catalogName}: REGEXP alternation should match 'string1'")
            assertTrue(regexpAlt.collect { it[0] }.contains("string3"),
                "${catalogName}: REGEXP alternation should match 'string3'")

            // LIKE should still work correctly (uses wildcard query, which is correct for LIKE)
            def likeResult = sql """select test1 from test1 where test1 like 'string%' order by test1;"""
            assertTrue(likeResult.size() >= 3,
                "${catalogName}: LIKE 'string%' should match at least 3 rows, got ${likeResult.size()}")

            // ==================================================================
            // P1-3: IS NULL predicate
            // Before fix: IS NULL generated {term: {field: null}} which returns 0
            // results in ES (ES doesn't index nulls as term values).
            // After fix: IS NULL generates {bool: {must_not: [{exists: {field: ...}}]}}
            // which correctly finds documents where the field is missing/null.
            // ==================================================================

            // IS NULL + IS NOT NULL should equal total count (partition completeness)
            def totalCount = sql """select count(*) from test1;"""
            def notNullCount = sql """select count(*) from test1 where message is not null;"""
            def isNullCount = sql """select count(*) from test1 where message is null;"""
            assertTrue(totalCount[0][0] == notNullCount[0][0] + isNullCount[0][0],
                "${catalogName}: IS NULL (${isNullCount[0][0]}) + IS NOT NULL (${notNullCount[0][0]}) should equal total (${totalCount[0][0]})")

            // IS NOT NULL should return non-empty results
            assertTrue(notNullCount[0][0] > 0,
                "${catalogName}: IS NOT NULL count should be > 0")
        }

        // Test on ES 7
        testEsPredicate("es7_pred_test")

        // Test on ES 8
        testEsPredicate("es8_pred_test")

        // cleanup
        sql """drop catalog if exists es7_pred_test;"""
        sql """drop catalog if exists es8_pred_test;"""
    }
}
