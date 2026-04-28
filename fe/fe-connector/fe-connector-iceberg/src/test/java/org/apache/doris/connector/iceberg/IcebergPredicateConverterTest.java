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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLike;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

class IcebergPredicateConverterTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amt", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(4, "d", Types.DateType.get()),
            Types.NestedField.optional(5, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(6, "lng", Types.LongType.get()));

    private static final ConnectorType T_INT = ConnectorType.of("INT");
    private static final ConnectorType T_BIGINT = ConnectorType.of("BIGINT");
    private static final ConnectorType T_STRING = ConnectorType.of("STRING");
    private static final ConnectorType T_DATE = ConnectorType.of("DATEV2");
    private static final ConnectorType T_DT = ConnectorType.of("DATETIMEV2");
    private static final ConnectorType T_DEC = ConnectorType.of("DECIMALV3", 10, 2);

    private static ConnectorColumnRef col(String name, ConnectorType t) {
        return new ConnectorColumnRef(name, t);
    }

    private static ConnectorLiteral litInt(int v) {
        return ConnectorLiteral.ofInt(v);
    }

    private static Expression convert(ConnectorExpression e) {
        return IcebergPredicateConverter.convert(e, SCHEMA);
    }

    // ---- 1. Comparisons ----

    @Test
    void eqIntColumn() {
        ConnectorExpression e = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("id", T_INT), litInt(5));
        Expression r = convert(e);
        Assertions.assertEquals(Operation.EQ, r.op());
        Assertions.assertTrue(r.toString().contains("id"));
        Assertions.assertTrue(r.toString().contains("5"));
    }

    @Test
    void neqStringColumn() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.NE,
                col("name", T_STRING), ConnectorLiteral.ofString("abc")));
        Assertions.assertEquals(Operation.NOT_EQ, r.op());
    }

    @Test
    void ltDateColumn() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.LT,
                col("d", T_DATE), ConnectorLiteral.ofDate(LocalDate.of(2024, 1, 2))));
        Assertions.assertEquals(Operation.LT, r.op());
    }

    @Test
    void leDecimalColumn() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.LE,
                col("amt", T_DEC), ConnectorLiteral.ofDecimal(new BigDecimal("12.34"), 10, 2)));
        Assertions.assertEquals(Operation.LT_EQ, r.op());
    }

    @Test
    void gtBigintColumn() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.GT,
                col("lng", T_BIGINT), ConnectorLiteral.ofLong(100L)));
        Assertions.assertEquals(Operation.GT, r.op());
    }

    @Test
    void geTimestampColumn() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.GE,
                col("ts", T_DT), ConnectorLiteral.ofDatetime(LocalDateTime.of(2024, 1, 1, 0, 0))));
        Assertions.assertEquals(Operation.GT_EQ, r.op());
    }

    @Test
    void mirroredLiteralOnLeft() {
        // 5 < id  ==>  id > 5
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.LT,
                litInt(5), col("id", T_INT)));
        Assertions.assertEquals(Operation.GT, r.op());
    }

    @Test
    void eqForNullOnNullLiteralBecomesIsNull() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.EQ_FOR_NULL,
                col("id", T_INT), ConnectorLiteral.ofNull(T_INT)));
        Assertions.assertEquals(Operation.IS_NULL, r.op());
    }

    // ---- 2. Logical ----

    @Test
    void andComposition() {
        ConnectorExpression a = new ConnectorComparison(ConnectorComparison.Operator.GT,
                col("id", T_INT), litInt(1));
        ConnectorExpression b = new ConnectorComparison(ConnectorComparison.Operator.LT,
                col("id", T_INT), litInt(10));
        Expression r = convert(new ConnectorAnd(Arrays.asList(a, b)));
        Assertions.assertEquals(Operation.AND, r.op());
    }

    @Test
    void orComposition() {
        ConnectorExpression a = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("id", T_INT), litInt(1));
        ConnectorExpression b = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("id", T_INT), litInt(2));
        Expression r = convert(new ConnectorOr(Arrays.asList(a, b)));
        Assertions.assertEquals(Operation.OR, r.op());
    }

    @Test
    void notComposition() {
        ConnectorExpression inner = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("id", T_INT), litInt(7));
        Expression r = convert(new ConnectorNot(inner));
        // Iceberg `not(equal)` is normalised to NOT_EQ.
        Assertions.assertTrue(r.op() == Operation.NOT_EQ || r.op() == Operation.NOT,
                "op was " + r.op());
    }

    @Test
    void nestedAndOrNot() {
        ConnectorExpression cmp = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("name", T_STRING), ConnectorLiteral.ofString("x"));
        ConnectorExpression or = new ConnectorOr(Arrays.asList(cmp,
                new ConnectorComparison(ConnectorComparison.Operator.EQ,
                        col("name", T_STRING), ConnectorLiteral.ofString("y"))));
        ConnectorExpression not = new ConnectorNot(new ConnectorComparison(
                ConnectorComparison.Operator.EQ, col("id", T_INT), litInt(0)));
        Expression r = convert(new ConnectorAnd(Arrays.asList(or, not)));
        Assertions.assertEquals(Operation.AND, r.op());
    }

    @Test
    void orWithUnconvertibleDegradesWholeOrToTrue() {
        ConnectorExpression good = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("id", T_INT), litInt(1));
        // Unknown column → alwaysTrue, so OR collapses to alwaysTrue.
        ConnectorExpression bad = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("nope", T_INT), litInt(2));
        Expression r = convert(new ConnectorOr(Arrays.asList(good, bad)));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    // ---- 3. IN / NOT IN ----

    @Test
    void inLiteralList() {
        Expression r = convert(new ConnectorIn(col("id", T_INT),
                Arrays.asList(litInt(1), litInt(2), litInt(3)), false));
        Assertions.assertEquals(Operation.IN, r.op());
    }

    @Test
    void notInLiteralList() {
        Expression r = convert(new ConnectorIn(col("name", T_STRING),
                Arrays.asList(ConnectorLiteral.ofString("a"), ConnectorLiteral.ofString("b")),
                true));
        Assertions.assertEquals(Operation.NOT_IN, r.op());
    }

    @Test
    void emptyInListDegradesToAlwaysTrue() {
        Expression r = convert(new ConnectorIn(col("id", T_INT),
                Collections.emptyList(), false));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    @Test
    void inWithNullLiteralDegrades() {
        Expression r = convert(new ConnectorIn(col("id", T_INT),
                Arrays.asList(litInt(1), ConnectorLiteral.ofNull(T_INT)), false));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    // ---- 4. Null tests ----

    @Test
    void isNull() {
        Expression r = convert(new ConnectorIsNull(col("name", T_STRING), false));
        Assertions.assertEquals(Operation.IS_NULL, r.op());
    }

    @Test
    void isNotNull() {
        Expression r = convert(new ConnectorIsNull(col("name", T_STRING), true));
        Assertions.assertEquals(Operation.NOT_NULL, r.op());
    }

    // ---- 5. STARTS_WITH / LIKE ----

    @Test
    void startsWithFunctionCall() {
        ConnectorFunctionCall sw = new ConnectorFunctionCall("starts_with", T_INT,
                Arrays.asList(col("name", T_STRING), ConnectorLiteral.ofString("foo")));
        Expression r = convert(sw);
        Assertions.assertEquals(Operation.STARTS_WITH, r.op());
    }

    @Test
    void likePrefixPercentBecomesStartsWith() {
        Expression r = convert(new ConnectorLike(ConnectorLike.Operator.LIKE,
                col("name", T_STRING), ConnectorLiteral.ofString("foo%")));
        Assertions.assertEquals(Operation.STARTS_WITH, r.op());
    }

    @Test
    void likePercentInfixDegrades() {
        Expression r = convert(new ConnectorLike(ConnectorLike.Operator.LIKE,
                col("name", T_STRING), ConnectorLiteral.ofString("%foo")));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    @Test
    void likeMidPercentDegrades() {
        Expression r = convert(new ConnectorLike(ConnectorLike.Operator.LIKE,
                col("name", T_STRING), ConnectorLiteral.ofString("foo%bar")));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    @Test
    void likeUnderscoreDegrades() {
        Expression r = convert(new ConnectorLike(ConnectorLike.Operator.LIKE,
                col("name", T_STRING), ConnectorLiteral.ofString("foo_bar")));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    @Test
    void regexpDegrades() {
        Expression r = convert(new ConnectorLike(ConnectorLike.Operator.REGEXP,
                col("name", T_STRING), ConnectorLiteral.ofString("foo.*")));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    // ---- 6. Degradation cases ----

    @Test
    void unknownColumnDegrades() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("ghost", T_INT), litInt(5)));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    @Test
    void literalTypeMismatchDegrades() {
        // BIGINT column with String literal → toIcebergLiteral returns null → alwaysTrue.
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("lng", T_BIGINT), ConnectorLiteral.ofString("not a number")));
        Assertions.assertEquals(Operation.TRUE, r.op());
    }

    @Test
    void nullExpressionInputDegrades() {
        Assertions.assertEquals(Operation.TRUE, convert(null).op());
    }

    @Test
    void betweenWithLiteralBoundsConverts() {
        Expression r = convert(new ConnectorBetween(col("id", T_INT), litInt(1), litInt(10)));
        Assertions.assertEquals(Operation.AND, r.op());
    }

    @Test
    void unsupportedFunctionCallDegrades() {
        ConnectorFunctionCall bogus = new ConnectorFunctionCall("year", T_INT,
                Collections.singletonList(col("d", T_DATE)));
        Assertions.assertEquals(Operation.TRUE, convert(bogus).op());
    }

    // ---- 7. convertAll ----

    @Test
    void convertAllNullReturnsAlwaysTrue() {
        Assertions.assertEquals(Operation.TRUE,
                IcebergPredicateConverter.convertAll(null, SCHEMA).op());
    }

    @Test
    void convertAllEmptyReturnsAlwaysTrue() {
        Assertions.assertEquals(Operation.TRUE,
                IcebergPredicateConverter.convertAll(Collections.emptyList(), SCHEMA).op());
    }

    @Test
    void convertAllJoinsConjuncts() {
        ConnectorExpression a = new ConnectorComparison(ConnectorComparison.Operator.GT,
                col("id", T_INT), litInt(1));
        ConnectorExpression b = new ConnectorComparison(ConnectorComparison.Operator.LT,
                col("id", T_INT), litInt(10));
        Expression r = IcebergPredicateConverter.convertAll(Arrays.asList(a, b), SCHEMA);
        // alwaysTrue AND a AND b is normalised by Iceberg's `Expressions.and`.
        Assertions.assertNotNull(r);
        Assertions.assertNotEquals(Operation.TRUE, r.op());
    }

    @Test
    void convertReturnsAlwaysTrueForBareLiteral() {
        // A bare literal at the top has no iceberg counterpart.
        Assertions.assertEquals(Operation.TRUE,
                convert(ConnectorLiteral.ofBoolean(true)).op());
    }

    @Test
    void caseInsensitiveColumnLookup() {
        Expression r = convert(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                col("ID", T_INT), litInt(5)));
        Assertions.assertEquals(Operation.EQ, r.op());
    }
}
