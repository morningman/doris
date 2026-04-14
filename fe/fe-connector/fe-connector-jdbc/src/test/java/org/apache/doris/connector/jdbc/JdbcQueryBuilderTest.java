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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for {@link JdbcQueryBuilder}, focusing on LIMIT pushdown
 * correctness when filters are partially pushable.
 */
class JdbcQueryBuilderTest {

    private static final ConnectorType INT_TYPE = ConnectorType.of("INT");
    private static final ConnectorType STRING_TYPE = ConnectorType.of("VARCHAR");

    private static final String DB = "testdb";
    private static final String TABLE = "testtbl";

    private JdbcQueryBuilder mysqlBuilder() {
        return new JdbcQueryBuilder(JdbcDbType.MYSQL);
    }

    private JdbcQueryBuilder oracleBuilder() {
        return new JdbcQueryBuilder(JdbcDbType.ORACLE);
    }

    private JdbcQueryBuilder sqlserverBuilder() {
        return new JdbcQueryBuilder(JdbcDbType.SQLSERVER);
    }

    private List<ConnectorColumnHandle> columns(String... names) {
        ConnectorColumnHandle[] cols = new ConnectorColumnHandle[names.length];
        for (int i = 0; i < names.length; i++) {
            cols[i] = new JdbcColumnHandle(names[i], names[i]);
        }
        return Arrays.asList(cols);
    }

    private ConnectorExpression simpleComparison(String col, int value) {
        return new ConnectorComparison(
                ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef(col, INT_TYPE),
                ConnectorLiteral.ofInt(value));
    }

    // A function call expression that cannot be pushed down (no functionConfig)
    private ConnectorExpression unpushableFunction(String col) {
        return new ConnectorComparison(
                ConnectorComparison.Operator.GT,
                new ConnectorFunctionCall("some_unknown_func",
                        INT_TYPE,
                        Collections.singletonList(new ConnectorColumnRef(col, INT_TYPE))),
                ConnectorLiteral.ofInt(0));
    }

    // --- LIMIT pushdown tests ---

    @Test
    void testLimitPushedWhenNoFilter() {
        JdbcQueryBuilder builder = mysqlBuilder();
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.empty(), 10);
        Assertions.assertTrue(sql.contains("LIMIT 10"),
                "LIMIT should be pushed when there is no filter. SQL: " + sql);
    }

    @Test
    void testLimitPushedWhenAllFiltersPushable() {
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE id = 1 AND name = 'test'
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                new ConnectorComparison(
                        ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("name", STRING_TYPE),
                        ConnectorLiteral.ofString("test"))));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 10);
        Assertions.assertTrue(sql.contains("LIMIT 10"),
                "LIMIT should be pushed when all filters are pushable. SQL: " + sql);
        Assertions.assertTrue(sql.contains("WHERE"),
                "WHERE clause should be present. SQL: " + sql);
    }

    @Test
    void testLimitNotPushedWhenPartialFiltersFail() {
        // No functionConfig → function calls cannot be pushed down
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE id = 1 AND some_unknown_func(name) > 0
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                unpushableFunction("name")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 10);
        Assertions.assertFalse(sql.contains("LIMIT"),
                "LIMIT must NOT be pushed when some filters cannot be pushed. SQL: " + sql);
        // The pushable part should still be in WHERE
        Assertions.assertTrue(sql.contains("WHERE"),
                "Pushable filter should still appear in WHERE. SQL: " + sql);
    }

    @Test
    void testLimitNotPushedWhenAllFiltersUnpushable() {
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE some_unknown_func(id) > 0 (single non-pushable expression)
        ConnectorExpression filter = unpushableFunction("id");
        String sql = builder.buildQuery(DB, TABLE, columns("id"),
                Optional.of(filter), 10);
        Assertions.assertFalse(sql.contains("LIMIT"),
                "LIMIT must NOT be pushed when no filters can be pushed. SQL: " + sql);
        Assertions.assertFalse(sql.contains("WHERE"),
                "WHERE should not appear since no filter was pushed. SQL: " + sql);
    }

    @Test
    void testNoLimitWhenLimitNegative() {
        JdbcQueryBuilder builder = mysqlBuilder();
        String sql = builder.buildQuery(DB, TABLE, columns("id"),
                Optional.empty(), -1);
        Assertions.assertFalse(sql.contains("LIMIT"),
                "No LIMIT clause when limit is -1. SQL: " + sql);
    }

    @Test
    void testOracleRownumNotAddedWhenPartialFiltersFail() {
        JdbcQueryBuilder builder = oracleBuilder();
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                unpushableFunction("name")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 5);
        Assertions.assertFalse(sql.contains("ROWNUM"),
                "Oracle ROWNUM must NOT be added when filters are partial. SQL: " + sql);
    }

    @Test
    void testOracleRownumAddedWhenAllFiltersPushable() {
        JdbcQueryBuilder builder = oracleBuilder();
        ConnectorExpression filter = simpleComparison("id", 1);
        String sql = builder.buildQuery(DB, TABLE, columns("id"),
                Optional.of(filter), 5);
        Assertions.assertTrue(sql.contains("ROWNUM <= 5"),
                "Oracle ROWNUM should be added when all filters are pushable. SQL: " + sql);
    }

    @Test
    void testSqlServerTopNotAddedWhenPartialFiltersFail() {
        JdbcQueryBuilder builder = sqlserverBuilder();
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                unpushableFunction("name")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 5);
        Assertions.assertFalse(sql.contains("TOP"),
                "SQL Server TOP must NOT be added when filters are partial. SQL: " + sql);
    }

    // --- Basic query generation tests ---

    @Test
    void testSelectAllNoFilterNoLimit() {
        JdbcQueryBuilder builder = mysqlBuilder();
        String sql = builder.buildQuery(DB, TABLE, Collections.emptyList(),
                Optional.empty(), -1);
        Assertions.assertEquals("SELECT * FROM `testdb`.`testtbl`", sql);
    }

    @Test
    void testSelectColumnsWithFilter() {
        JdbcQueryBuilder builder = mysqlBuilder();
        ConnectorExpression filter = simpleComparison("id", 42);
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), -1);
        Assertions.assertTrue(sql.contains("WHERE"),
                "Simple comparison should be pushed. SQL: " + sql);
        Assertions.assertTrue(sql.contains("`id`") && sql.contains("`name`"),
                "Column names should be quoted. SQL: " + sql);
    }

    @Test
    void testNestedAndWithPartialFailure() {
        JdbcQueryBuilder builder = mysqlBuilder();
        // WHERE (id = 1 AND name = 'x') AND unknownFunc(id) > 0
        // The first two are pushable, the third is not
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                simpleComparison("id", 1),
                new ConnectorComparison(
                        ConnectorComparison.Operator.EQ,
                        new ConnectorColumnRef("name", STRING_TYPE),
                        ConnectorLiteral.ofString("x")),
                unpushableFunction("id")));
        String sql = builder.buildQuery(DB, TABLE, columns("id", "name"),
                Optional.of(filter), 10);
        // Pushable filters should be present
        Assertions.assertTrue(sql.contains("WHERE"),
                "Pushable filters should still be in WHERE. SQL: " + sql);
        // LIMIT must NOT be pushed
        Assertions.assertFalse(sql.contains("LIMIT"),
                "LIMIT must NOT be pushed with partial filter failure. SQL: " + sql);
    }
}
