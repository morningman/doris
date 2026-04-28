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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Converts a {@link ConnectorExpression} tree to an Iceberg
 * {@link Expression} suitable for {@code TableScan.filter(...)}.
 *
 * <p>Plugin-side equivalent of the legacy
 * {@code IcebergUtils.convertToIcebergExpr} that lives in fe-core. Unlike the
 * legacy code it never references Doris analysis types; the input grammar is
 * the {@code ConnectorExpression} SPI defined in {@code fe-connector-api}.
 *
 * <p>Conversion rules (see plan-doc D11 §6.4):
 * <ul>
 *   <li>Logical: {@code AND}, {@code OR}, {@code NOT}.</li>
 *   <li>Comparison: {@code = != &lt; &lt;= &gt; &gt;=} on {@code (column, literal)}.
 *       The SPI {@code EQ_FOR_NULL} is mapped to {@code isNull(col)} when the
 *       literal is {@code NULL}, otherwise to {@code equal(col, value)}.</li>
 *   <li>Set membership: {@code IN} / {@code NOT IN}; an empty {@code IN} list
 *       degrades to {@link Expressions#alwaysTrue()} (conservative; never
 *       produces {@code alwaysFalse()} per D11 conservative rule).</li>
 *   <li>Null tests: {@code IS NULL}, {@code IS NOT NULL}.</li>
 *   <li>String prefix: {@code STARTS_WITH(col, 'lit')} via
 *       {@link Expressions#startsWith(String, String)}; {@code LIKE} is
 *       converted only when the pattern is a strict {@code prefix%} literal
 *       (no other wildcards).</li>
 * </ul>
 *
 * <p>Anything not recognised — Cast subtrees, expressions on functions
 * other than {@code starts_with}, comparisons with both sides non-literal,
 * literal/column type mismatches, unknown column names, {@code REGEXP},
 * {@code BETWEEN} on non-literal bounds, etc. — degrades the affected
 * subtree to {@link Expressions#alwaysTrue()}. Iceberg's scan filter is an
 * over-approximation, so {@code alwaysTrue} just means "let the plan node
 * filter rows after the scan".
 */
public final class IcebergPredicateConverter {

    private IcebergPredicateConverter() {
    }

    /**
     * Convert a single {@link ConnectorExpression} subtree to an Iceberg
     * {@link Expression}. Returns {@link Expressions#alwaysTrue()} for
     * {@code null} input or any unconvertible subtree.
     */
    public static Expression convert(ConnectorExpression expr, Schema icebergSchema) {
        Objects.requireNonNull(icebergSchema, "icebergSchema");
        if (expr == null) {
            return Expressions.alwaysTrue();
        }
        return new Visitor(icebergSchema).visit(expr);
    }

    /**
     * Convert a list of conjunct {@link ConnectorExpression}s, joining the
     * results with {@code AND}. Unconvertible conjuncts contribute
     * {@link Expressions#alwaysTrue()} (i.e. they are dropped from the
     * effective filter). Empty / {@code null} input returns
     * {@link Expressions#alwaysTrue()}.
     */
    public static Expression convertAll(List<ConnectorExpression> filters, Schema icebergSchema) {
        Objects.requireNonNull(icebergSchema, "icebergSchema");
        if (filters == null || filters.isEmpty()) {
            return Expressions.alwaysTrue();
        }
        Visitor visitor = new Visitor(icebergSchema);
        Expression acc = Expressions.alwaysTrue();
        for (ConnectorExpression f : filters) {
            Expression part = f == null ? Expressions.alwaysTrue() : visitor.visit(f);
            acc = Expressions.and(acc, part);
        }
        return acc;
    }

    /** Visitor dispatching on the {@code ConnectorExpression} subtype. */
    private static final class Visitor {
        private final Schema schema;

        Visitor(Schema schema) {
            this.schema = schema;
        }

        Expression visit(ConnectorExpression expr) {
            if (expr instanceof ConnectorAnd) {
                return visitAnd((ConnectorAnd) expr);
            }
            if (expr instanceof ConnectorOr) {
                return visitOr((ConnectorOr) expr);
            }
            if (expr instanceof ConnectorNot) {
                return visitNot((ConnectorNot) expr);
            }
            if (expr instanceof ConnectorComparison) {
                return visitComparison((ConnectorComparison) expr);
            }
            if (expr instanceof ConnectorIn) {
                return visitIn((ConnectorIn) expr);
            }
            if (expr instanceof ConnectorIsNull) {
                return visitIsNull((ConnectorIsNull) expr);
            }
            if (expr instanceof ConnectorLike) {
                return visitLike((ConnectorLike) expr);
            }
            if (expr instanceof ConnectorFunctionCall) {
                return visitFunctionCall((ConnectorFunctionCall) expr);
            }
            if (expr instanceof ConnectorBetween) {
                return visitBetween((ConnectorBetween) expr);
            }
            // ConnectorLiteral / ConnectorColumnRef as a top-level boolean,
            // Cast (no SPI subtype today), or anything else: cannot push down.
            return Expressions.alwaysTrue();
        }

        private Expression visitAnd(ConnectorAnd and) {
            Expression acc = Expressions.alwaysTrue();
            for (ConnectorExpression child : and.getConjuncts()) {
                acc = Expressions.and(acc, visit(child));
            }
            return acc;
        }

        private Expression visitOr(ConnectorOr or) {
            // OR requires every disjunct to be convertible; otherwise the
            // whole disjunction degrades to alwaysTrue (never alwaysFalse).
            List<ConnectorExpression> ds = or.getDisjuncts();
            if (ds.isEmpty()) {
                return Expressions.alwaysTrue();
            }
            Expression acc = null;
            for (ConnectorExpression child : ds) {
                Expression part = visit(child);
                if (isAlwaysTrue(part)) {
                    return Expressions.alwaysTrue();
                }
                acc = (acc == null) ? part : Expressions.or(acc, part);
            }
            return acc == null ? Expressions.alwaysTrue() : acc;
        }

        private Expression visitNot(ConnectorNot not) {
            Expression child = visit(not.getOperand());
            if (isAlwaysTrue(child)) {
                return Expressions.alwaysTrue();
            }
            return Expressions.not(child);
        }

        private Expression visitComparison(ConnectorComparison cmp) {
            ConnectorExpression left = cmp.getLeft();
            ConnectorExpression right = cmp.getRight();

            ConnectorColumnRef colRef = null;
            ConnectorLiteral literal = null;
            ConnectorComparison.Operator op = cmp.getOperator();

            if (left instanceof ConnectorColumnRef && right instanceof ConnectorLiteral) {
                colRef = (ConnectorColumnRef) left;
                literal = (ConnectorLiteral) right;
            } else if (right instanceof ConnectorColumnRef && left instanceof ConnectorLiteral) {
                colRef = (ConnectorColumnRef) right;
                literal = (ConnectorLiteral) left;
                op = mirror(op);
            } else {
                return Expressions.alwaysTrue();
            }

            Types.NestedField field = schema.caseInsensitiveFindField(colRef.getColumnName());
            if (field == null) {
                return Expressions.alwaysTrue();
            }
            String name = field.name();

            if (literal.isNull()) {
                if (op == ConnectorComparison.Operator.EQ_FOR_NULL) {
                    return Expressions.isNull(name);
                }
                // x = NULL etc. is unknown; let the plan node handle it.
                return Expressions.alwaysTrue();
            }

            Object value = toIcebergLiteral(literal, field.type());
            if (value == null) {
                return Expressions.alwaysTrue();
            }
            switch (op) {
                case EQ:
                case EQ_FOR_NULL:
                    return Expressions.equal(name, value);
                case NE:
                    return Expressions.notEqual(name, value);
                case LT:
                    return Expressions.lessThan(name, value);
                case LE:
                    return Expressions.lessThanOrEqual(name, value);
                case GT:
                    return Expressions.greaterThan(name, value);
                case GE:
                    return Expressions.greaterThanOrEqual(name, value);
                default:
                    return Expressions.alwaysTrue();
            }
        }

        private Expression visitIn(ConnectorIn in) {
            ConnectorExpression valueExpr = in.getValue();
            if (!(valueExpr instanceof ConnectorColumnRef)) {
                return Expressions.alwaysTrue();
            }
            ConnectorColumnRef colRef = (ConnectorColumnRef) valueExpr;
            Types.NestedField field = schema.caseInsensitiveFindField(colRef.getColumnName());
            if (field == null) {
                return Expressions.alwaysTrue();
            }
            String name = field.name();

            List<ConnectorExpression> items = in.getInList();
            // Empty IN list: conservatively alwaysTrue (D11 §6.4 — never
            // alwaysFalse, even though strictly `x IN ()` is FALSE; the
            // post-scan plan node will short-circuit).
            if (items.isEmpty()) {
                return Expressions.alwaysTrue();
            }
            List<Object> values = new ArrayList<>(items.size());
            for (ConnectorExpression item : items) {
                if (!(item instanceof ConnectorLiteral)) {
                    return Expressions.alwaysTrue();
                }
                ConnectorLiteral lit = (ConnectorLiteral) item;
                if (lit.isNull()) {
                    return Expressions.alwaysTrue();
                }
                Object v = toIcebergLiteral(lit, field.type());
                if (v == null) {
                    return Expressions.alwaysTrue();
                }
                values.add(v);
            }
            return in.isNegated()
                    ? Expressions.notIn(name, values)
                    : Expressions.in(name, values);
        }

        private Expression visitIsNull(ConnectorIsNull isNull) {
            ConnectorExpression operand = isNull.getOperand();
            if (!(operand instanceof ConnectorColumnRef)) {
                return Expressions.alwaysTrue();
            }
            Types.NestedField field = schema.caseInsensitiveFindField(
                    ((ConnectorColumnRef) operand).getColumnName());
            if (field == null) {
                return Expressions.alwaysTrue();
            }
            return isNull.isNegated()
                    ? Expressions.notNull(field.name())
                    : Expressions.isNull(field.name());
        }

        private Expression visitLike(ConnectorLike like) {
            if (like.getOperator() != ConnectorLike.Operator.LIKE) {
                return Expressions.alwaysTrue();
            }
            ConnectorExpression valueExpr = like.getValue();
            ConnectorExpression patternExpr = like.getPattern();
            if (!(valueExpr instanceof ConnectorColumnRef)
                    || !(patternExpr instanceof ConnectorLiteral)) {
                return Expressions.alwaysTrue();
            }
            ConnectorLiteral patternLit = (ConnectorLiteral) patternExpr;
            if (patternLit.isNull() || !(patternLit.getValue() instanceof String)) {
                return Expressions.alwaysTrue();
            }
            String pattern = (String) patternLit.getValue();
            String prefix = strictPrefixOrNull(pattern);
            if (prefix == null) {
                return Expressions.alwaysTrue();
            }
            Types.NestedField field = schema.caseInsensitiveFindField(
                    ((ConnectorColumnRef) valueExpr).getColumnName());
            if (field == null || field.type().typeId() != Type.TypeID.STRING) {
                return Expressions.alwaysTrue();
            }
            return Expressions.startsWith(field.name(), prefix);
        }

        private Expression visitFunctionCall(ConnectorFunctionCall call) {
            String fn = call.getFunctionName();
            if (fn == null) {
                return Expressions.alwaysTrue();
            }
            if (!"starts_with".equalsIgnoreCase(fn) && !"startswith".equalsIgnoreCase(fn)) {
                return Expressions.alwaysTrue();
            }
            List<ConnectorExpression> args = call.getArguments();
            if (args.size() != 2) {
                return Expressions.alwaysTrue();
            }
            ConnectorExpression a0 = args.get(0);
            ConnectorExpression a1 = args.get(1);
            if (!(a0 instanceof ConnectorColumnRef) || !(a1 instanceof ConnectorLiteral)) {
                return Expressions.alwaysTrue();
            }
            ConnectorLiteral lit = (ConnectorLiteral) a1;
            if (lit.isNull() || !(lit.getValue() instanceof String)) {
                return Expressions.alwaysTrue();
            }
            Types.NestedField field = schema.caseInsensitiveFindField(
                    ((ConnectorColumnRef) a0).getColumnName());
            if (field == null || field.type().typeId() != Type.TypeID.STRING) {
                return Expressions.alwaysTrue();
            }
            return Expressions.startsWith(field.name(), (String) lit.getValue());
        }

        private Expression visitBetween(ConnectorBetween between) {
            // BETWEEN value lower upper => lower <= value <= upper
            ConnectorExpression v = between.getValue();
            ConnectorExpression lo = between.getLower();
            ConnectorExpression hi = between.getUpper();
            if (!(v instanceof ConnectorColumnRef)
                    || !(lo instanceof ConnectorLiteral)
                    || !(hi instanceof ConnectorLiteral)) {
                return Expressions.alwaysTrue();
            }
            Types.NestedField field = schema.caseInsensitiveFindField(
                    ((ConnectorColumnRef) v).getColumnName());
            if (field == null) {
                return Expressions.alwaysTrue();
            }
            ConnectorLiteral lowLit = (ConnectorLiteral) lo;
            ConnectorLiteral highLit = (ConnectorLiteral) hi;
            if (lowLit.isNull() || highLit.isNull()) {
                return Expressions.alwaysTrue();
            }
            Object lowV = toIcebergLiteral(lowLit, field.type());
            Object highV = toIcebergLiteral(highLit, field.type());
            if (lowV == null || highV == null) {
                return Expressions.alwaysTrue();
            }
            return Expressions.and(
                    Expressions.greaterThanOrEqual(field.name(), lowV),
                    Expressions.lessThanOrEqual(field.name(), highV));
        }
    }

    /**
     * Returns the literal prefix iff {@code pattern} is exactly {@code prefix%}
     * with no other wildcards (no leading {@code %}, no embedded {@code %},
     * no {@code _}, no escapes). Otherwise {@code null}.
     */
    private static String strictPrefixOrNull(String pattern) {
        if (pattern.isEmpty() || !pattern.endsWith("%")) {
            return null;
        }
        String body = pattern.substring(0, pattern.length() - 1);
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '%' || c == '_' || c == '\\') {
                return null;
            }
        }
        return body;
    }

    private static ConnectorComparison.Operator mirror(ConnectorComparison.Operator op) {
        switch (op) {
            case LT:
                return ConnectorComparison.Operator.GT;
            case LE:
                return ConnectorComparison.Operator.GE;
            case GT:
                return ConnectorComparison.Operator.LT;
            case GE:
                return ConnectorComparison.Operator.LE;
            default:
                return op;
        }
    }

    private static boolean isAlwaysTrue(Expression expr) {
        return expr.op() == Expression.Operation.TRUE;
    }

    /**
     * Convert a {@link ConnectorLiteral} to a Java value compatible with the
     * iceberg {@link Expressions} factory for the supplied iceberg type.
     * Returns {@code null} if no clean conversion is possible (caller will
     * degrade the subtree to {@link Expressions#alwaysTrue()}).
     *
     * <p>Date / timestamp encoding follows iceberg's internal contract:
     * {@code DATE} → days since epoch ({@code Integer}); {@code TIMESTAMP}
     * (with or without zone) → micros since epoch ({@code Long}). Since the
     * SPI carries only {@code LocalDate} / {@code LocalDateTime} (no zone),
     * timestamp literals are interpreted as UTC for both adjust-to-UTC and
     * non-adjust columns; the legacy fe-core converter uses session TZ for
     * adjust-to-UTC, but the SPI does not yet expose session TZ — this is
     * tracked for M2-Iceberg-04 / 07.
     */
    static Object toIcebergLiteral(ConnectorLiteral literal, Type icebergType) {
        Object v = literal.getValue();
        if (v == null) {
            return null;
        }
        Type.TypeID id = icebergType.typeId();
        switch (id) {
            case BOOLEAN:
                return v instanceof Boolean ? v : null;
            case INTEGER:
                if (v instanceof Number) {
                    return ((Number) v).intValue();
                }
                return null;
            case LONG:
                if (v instanceof Number) {
                    return ((Number) v).longValue();
                }
                return null;
            case FLOAT:
                if (v instanceof Number) {
                    return ((Number) v).floatValue();
                }
                return null;
            case DOUBLE:
                if (v instanceof Number) {
                    return ((Number) v).doubleValue();
                }
                return null;
            case DECIMAL:
                if (v instanceof BigDecimal) {
                    Types.DecimalType dt = (Types.DecimalType) icebergType;
                    BigDecimal bd = (BigDecimal) v;
                    if (bd.scale() == dt.scale()) {
                        return bd;
                    }
                    return bd.setScale(dt.scale(), java.math.RoundingMode.UNNECESSARY);
                }
                if (v instanceof Number) {
                    Types.DecimalType dt = (Types.DecimalType) icebergType;
                    return BigDecimal.valueOf(((Number) v).doubleValue())
                            .setScale(dt.scale(), java.math.RoundingMode.HALF_UP);
                }
                return null;
            case STRING:
                return v.toString();
            case DATE:
                if (v instanceof LocalDate) {
                    return (int) ((LocalDate) v).toEpochDay();
                }
                if (v instanceof CharSequence) {
                    return (int) LocalDate.parse(v.toString()).toEpochDay();
                }
                return null;
            case TIMESTAMP:
                if (v instanceof LocalDateTime) {
                    return toMicros((LocalDateTime) v);
                }
                if (v instanceof LocalDate) {
                    return toMicros(((LocalDate) v).atStartOfDay());
                }
                return null;
            case BINARY:
            case FIXED:
                if (v instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) v);
                }
                if (v instanceof CharSequence) {
                    return ByteBuffer.wrap(v.toString().getBytes(StandardCharsets.UTF_8));
                }
                return null;
            case UUID:
                if (v instanceof java.util.UUID) {
                    return v;
                }
                if (v instanceof CharSequence) {
                    return java.util.UUID.fromString(v.toString());
                }
                return null;
            default:
                return null;
        }
    }

    private static long toMicros(LocalDateTime dt) {
        long secs = dt.toEpochSecond(ZoneOffset.UTC);
        return secs * 1_000_000L + dt.getNano() / 1000L;
    }
}
