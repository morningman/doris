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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.DiffOutputInAsterisk;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Logical filter plan.
 */
public class LogicalFilter<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements Filter, DiffOutputInAsterisk {

    private final Set<Expression> conjuncts;
    private final Supplier<Expression> predicate;

    public LogicalFilter(Set<Expression> conjuncts, CHILD_TYPE child) {
        this(conjuncts, null, Optional.empty(), Optional.empty(), child);
    }

    public LogicalFilter(Set<Expression> conjuncts, Expression andConjuncts, CHILD_TYPE child) {
        this(conjuncts, () -> andConjuncts, Optional.empty(), Optional.empty(), child);
    }

    private LogicalFilter(Set<Expression> conjuncts,
            Supplier<Expression> predicate, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_FILTER, groupExpression, logicalProperties, child);
        this.conjuncts = Utils.fastToImmutableSet(Objects.requireNonNull(conjuncts, "conjuncts can not be null"));
        this.predicate = predicate == null ? Suppliers.memoize(Filter.super::getPredicate) : predicate;
        if (Utils.enableAssert) {
            for (Expression conjunct : conjuncts) {
                if (conjunct instanceof And) {
                    throw new IllegalStateException("Filter conjuncts can not contain and: " + conjunct.toSql());
                }
            }
        }
    }

    @Override
    public Set<Expression> getConjuncts() {
        return conjuncts;
    }

    @Override
    public Expression getPredicate() {
        return predicate.get();
    }

    @Override
    public List<Slot> getOutput() {
        return child().getOutput();
    }

    public List<Expression> getExpressions() {
        return Utils.fastToImmutableList(conjuncts);
    }

    @Override
    public List<? extends Plan> extraPlans() {
        return conjuncts.stream().map(Expression::children).flatMap(Collection::stream).flatMap(m -> {
            if (m instanceof SubqueryExpr) {
                return Stream.of(new LogicalSubQueryAlias<>(m.toSql(), ((SubqueryExpr) m).getQueryPlan()));
            } else {
                return new LogicalFilter<Plan>(ExpressionUtils.extractConjunctionToSet(m), child())
                        .extraPlans().stream();
            }
        }).collect(Collectors.toList());
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public List<Slot> computeAsteriskOutput() {
        return child().getAsteriskOutput();
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalFilter[" + id.asInt() + "]",
                "predicates", getPredicate(),
                "stats", statistics
        );
    }

    @Override
    public String getFingerprint() {
        return Utils.toSqlString("Filter[" + getGroupIdWithPrefix() + "]",
                "predicates", getPredicate().getFingerprint()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalFilter that = (LogicalFilter) o;
        return conjuncts.equals(that.conjuncts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conjuncts);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFilter(this, context);
    }

    public LogicalFilter<Plan> withConjuncts(Set<Expression> conjuncts) {
        return new LogicalFilter<>(conjuncts, null, Optional.empty(), Optional.of(getLogicalProperties()), child());
    }

    @Override
    public LogicalFilter<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFilter<>(conjuncts, predicate, Optional.empty(), Optional.empty(), children.get(0));
    }

    @Override
    public LogicalFilter<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFilter<>(conjuncts, predicate, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFilter<>(conjuncts, predicate, groupExpression, logicalProperties, children.get(0));
    }

    public LogicalFilter<Plan> withConjunctsAndChild(Set<Expression> conjuncts, Plan child) {
        return new LogicalFilter<>(conjuncts, child);
    }

    public LogicalFilter<Plan> withConjunctsAndProps(Set<Expression> conjuncts,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, Plan child) {
        return new LogicalFilter<>(conjuncts, null, groupExpression, logicalProperties, child);
    }

    @Override
    public void computeUnique(Builder builder) {
        builder.addUniqueSlot(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeUniform(Builder builder) {
        for (Expression e : getConjuncts()) {
            Map<Slot, Expression> uniformSlots = ExpressionUtils.extractUniformSlot(e);
            for (Map.Entry<Slot, Expression> entry : uniformSlots.entrySet()) {
                builder.addUniformSlotAndLiteral(entry.getKey(), entry.getValue());
            }
        }
        builder.addUniformSlot(child(0).getLogicalProperties().getTrait());
    }

    @Override
    public void computeEqualSet(Builder builder) {
        builder.addEqualSet(child().getLogicalProperties().getTrait());
        for (Expression expression : getConjuncts()) {
            Optional<Pair<Slot, Slot>> equalSlot = ExpressionUtils.extractEqualSlot(expression);
            equalSlot.ifPresent(slotSlotPair -> builder.addEqualPair(slotSlotPair.first, slotSlotPair.second));
        }
    }

    @Override
    public void computeFd(Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
