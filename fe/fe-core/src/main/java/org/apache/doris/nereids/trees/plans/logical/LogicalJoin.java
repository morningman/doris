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
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.DiffOutputInAsterisk;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.ImmutableEqualSet;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Logical join plan.
 */
public class LogicalJoin<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements Join, DiffOutputInAsterisk {

    private final JoinType joinType;
    private final List<Expression> otherJoinConjuncts;
    private final List<Expression> hashJoinConjuncts;
    private final List<Expression> markJoinConjuncts;
    private final List<Slot> exceptAsteriskOutputs;

    // When the predicate condition contains subqueries and disjunctions, the join will be marked as MarkJoin.
    private final Optional<MarkJoinSlotReference> markJoinSlotReference;

    // Use for top-to-down join reorder
    private final JoinReorderContext joinReorderContext = new JoinReorderContext();
    // Table bitmap for tables below this join
    private long bitmap = LongBitmap.newBitmap();

    private DistributeHint hint;

    public LogicalJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild,
                       JoinReorderContext otherJoinReorderContext) {
        this(joinType, ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION,
                ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE),
                Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableList.of(leftChild, rightChild), otherJoinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, JoinReorderContext otherJoinReorderContext) {
        this(joinType, hashJoinConjuncts, ExpressionUtils.EMPTY_CONDITION,
                ExpressionUtils.EMPTY_CONDITION, new DistributeHint(DistributeType.NONE),
                Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableList.of(leftChild, rightChild), otherJoinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, List<Expression> otherJoinConjuncts,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, JoinReorderContext otherJoinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE), Optional.empty(),
                Optional.empty(), Optional.empty(), ImmutableList.of(leftChild, rightChild), otherJoinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, DistributeHint hint, LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild, JoinReorderContext otherJoinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION, hint,
                Optional.empty(), Optional.empty(), Optional.empty(),
                ImmutableList.of(leftChild, rightChild), otherJoinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference, LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild, JoinReorderContext otherJoinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION, hint,
                markJoinSlotReference, Optional.empty(), Optional.empty(),
                ImmutableList.of(leftChild, rightChild), otherJoinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
                       List<Expression> otherJoinConjuncts, List<Expression> markJoinConjuncts, DistributeHint hint,
                       Optional<MarkJoinSlotReference> markJoinSlotReference, LEFT_CHILD_TYPE leftChild,
                       RIGHT_CHILD_TYPE rightChild, JoinReorderContext joinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts, hint,
                markJoinSlotReference, Optional.empty(), Optional.empty(),
                ImmutableList.of(leftChild, rightChild), joinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference, List<Plan> children,
            JoinReorderContext otherJoinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION, hint,
                markJoinSlotReference, Optional.empty(), Optional.empty(), children, otherJoinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference, List<Slot> exceptAsteriskOutputs,
            List<Plan> children, JoinReorderContext otherJoinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION, hint,
                markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), children, otherJoinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
                       List<Expression> otherJoinConjuncts, List<Expression> markJoinConjuncts, DistributeHint hint,
                       Optional<MarkJoinSlotReference> markJoinSlotReference, List<Plan> children,
                       JoinReorderContext otherJoinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts, hint,
                markJoinSlotReference, Optional.empty(), Optional.empty(), children, otherJoinReorderContext);
    }

    private LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, List<Expression> markJoinConjuncts,
            DistributeHint hint, Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children,
            JoinReorderContext joinReorderContext) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts, hint,
                markJoinSlotReference, ImmutableList.of(),
                groupExpression, logicalProperties, children, joinReorderContext);
    }

    private LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, List<Expression> markJoinConjuncts,
            DistributeHint hint, Optional<MarkJoinSlotReference> markJoinSlotReference,
            List<Slot> exceptAsteriskOutputs,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children,
            JoinReorderContext joinReorderContext) {
        // Just use in withXXX method. Don't need check/copyOf()
        super(PlanType.LOGICAL_JOIN, groupExpression, logicalProperties, children);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = Utils.fastToImmutableList(hashJoinConjuncts);
        this.otherJoinConjuncts = Utils.fastToImmutableList(otherJoinConjuncts);
        this.markJoinConjuncts = Utils.fastToImmutableList(markJoinConjuncts);
        this.hint = Objects.requireNonNull(hint, "hint can not be null");
        if (joinReorderContext != null) {
            this.joinReorderContext.copyFrom(joinReorderContext);
        }
        this.markJoinSlotReference = markJoinSlotReference;
        this.exceptAsteriskOutputs = exceptAsteriskOutputs;
    }

    public LogicalJoin<? extends Plan, ? extends Plan> swap() {
        return withTypeChildren(getJoinType().swap(),
                right(), left(), null);
    }

    public List<Expression> getOtherJoinConjuncts() {
        return otherJoinConjuncts;
    }

    public List<Expression> getHashJoinConjuncts() {
        return hashJoinConjuncts;
    }

    /**
     * getConditionSlot
     */
    public Set<Slot> getConditionSlot() {
        return Stream.concat(Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream()),
                markJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * getConditionExprId
     */
    public Set<ExprId> getConditionExprId() {
        return Stream.concat(Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream()),
                markJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlotExprIds().stream()).collect(Collectors.toSet());
    }

    /**
     * getLeftConditionSlot
     */
    public Set<Slot> getLeftConditionSlot() {
        // TODO this function is used by TransposeSemiJoinAgg, we assume it can handle mark join correctly.
        Set<Slot> leftOutputSet = this.left().getOutputSet();
        return Stream
                .concat(Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream()),
                        markJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream()).filter(leftOutputSet::contains)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * getOnClauseCondition
     */
    public Optional<Expression> getOnClauseCondition() {
        // TODO this function is called by AggScalarSubQueryToWindowFunction and InferPredicates
        //  we assume they can handle mark join correctly
        Optional<Expression> normalJoinConjuncts =
                ExpressionUtils.optionalAnd(hashJoinConjuncts, otherJoinConjuncts);
        return normalJoinConjuncts.isPresent()
                ? ExpressionUtils.optionalAnd(ImmutableList.of(normalJoinConjuncts.get()),
                        markJoinConjuncts)
                : ExpressionUtils.optionalAnd(markJoinConjuncts);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public DistributeHint getDistributeHint() {
        return hint;
    }

    public void setHint(DistributeHint hint) {
        this.hint = hint;
    }

    public boolean isMarkJoin() {
        return markJoinSlotReference.isPresent();
    }

    public boolean isLeadingJoin() {
        return joinReorderContext.isLeadingJoin();
    }

    public List<Expression> getMarkJoinConjuncts() {
        return markJoinConjuncts;
    }

    public JoinReorderContext getJoinReorderContext() {
        return joinReorderContext;
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(JoinUtils.getJoinOutput(joinType, left(), right()))
                .addAll(isMarkJoin()
                        ? ImmutableList.of(markJoinSlotReference.get()) : ImmutableList.of())
                .build();
    }

    @Override
    public List<Slot> computeAsteriskOutput() {
        List<Slot> output = Lists.newArrayList(JoinUtils.getJoinOutput(joinType, left(), right(), true));
        if (isMarkJoin()) {
            output.add(markJoinSlotReference.get());
        }
        output.removeAll(exceptAsteriskOutputs);
        return output;
    }

    @Override
    public List<Slot> getAsteriskOutput() {
        boolean outputIsDiff = !exceptAsteriskOutputs.isEmpty();
        return outputIsDiff ? getLogicalProperties().getAsteriskOutput()
                : DiffOutputInAsterisk.super.getAsteriskOutput();
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList(
                "type", joinType,
                "markJoinSlotReference", markJoinSlotReference,
                "hashJoinConjuncts", hashJoinConjuncts,
                "otherJoinConjuncts", otherJoinConjuncts,
                "markJoinConjuncts", markJoinConjuncts,
                "stats", statistics);
        if (hint.distributeType != DistributeType.NONE) {
            args.add("hint");
            args.add(hint.getExplainString());
        }
        return Utils.toSqlStringSkipNull("LogicalJoin[" + id.asInt() + "]", args.toArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalJoin<?, ?> that = (LogicalJoin<?, ?>) o;
        return joinType == that.joinType
                && hint.equals(that.hint)
                && hashJoinConjuncts.equals(that.hashJoinConjuncts)
                && otherJoinConjuncts.equals(that.otherJoinConjuncts)
                && markJoinConjuncts.equals(that.markJoinConjuncts)
                && exceptAsteriskOutputs.equals(that.exceptAsteriskOutputs)
                && Objects.equals(markJoinSlotReference, that.markJoinSlotReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, hashJoinConjuncts, otherJoinConjuncts,
                markJoinConjuncts, markJoinSlotReference, exceptAsteriskOutputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJoin(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(hashJoinConjuncts)
                .addAll(otherJoinConjuncts)
                .addAll(markJoinConjuncts)
                .build();
    }

    public Optional<MarkJoinSlotReference> getMarkJoinSlotReference() {
        return markJoinSlotReference;
    }

    public List<Slot> getExceptAsteriskOutputs() {
        return exceptAsteriskOutputs;
    }

    public long getBitmap() {
        return bitmap;
    }

    public void setBitmap(long bitmap) {
        this.bitmap = bitmap;
    }

    @Override
    public LEFT_CHILD_TYPE left() {
        return (LEFT_CHILD_TYPE) child(0);
    }

    @Override
    public RIGHT_CHILD_TYPE right() {
        return (RIGHT_CHILD_TYPE) child(1);
    }

    @Override
    public LogicalJoin<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), children, joinReorderContext);
    }

    @Override
    public LogicalJoin<Plan, Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                groupExpression, Optional.of(getLogicalProperties()), children, joinReorderContext);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                groupExpression, logicalProperties, children, joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withChildrenNoContext(Plan left, Plan right,
                                                         JoinReorderContext otherJoinReorderContext) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), ImmutableList.of(left, right), otherJoinReorderContext);
    }

    /**
     * Using in binding using join, and must set logical properties to empty.
     */
    public LogicalJoin<Plan, Plan> withJoinConjuncts(List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, JoinReorderContext otherJoinReorderContext) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), children, otherJoinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withJoinConjuncts(List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, List<Expression> markJoinConjuncts,
            JoinReorderContext otherJoinReorderContext) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.of(getLogicalProperties()), children, otherJoinReorderContext);
    }

    /**
     * Creates a new LogicalJoin with updated hash join conjuncts, mark join conjuncts, and child plans.
     *
     * @param hashJoinConjuncts the list of hash join conjuncts used for hash-based join conditions.
     * @param markJoinConjuncts the list of mark join conjuncts used for marking specific join conditions.
     *                          These are typically used in semi-join or anti-join scenarios to track
     *                          whether a condition is satisfied.
     * @param left the left child plan.
     * @param right the right child plan.
     * @param otherJoinReorderContext the context for join reordering.
     * @return a new LogicalJoin instance with the specified parameters.
     */
    public LogicalJoin<Plan, Plan> withHashAndMarkJoinConjunctsAndChildren(
            List<Expression> hashJoinConjuncts, List<Expression> markJoinConjuncts,
            Plan left, Plan right, JoinReorderContext otherJoinReorderContext) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), ImmutableList.of(left, right), otherJoinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withHashJoinConjuncts(List<Expression> hashJoinConjuncts) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(),
                ImmutableList.of(left(), right()), joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withConjunctsChildren(List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, Plan left, Plan right, JoinReorderContext otherJoinReorderContext) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), ImmutableList.of(left, right), otherJoinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withConjunctsChildren(List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts, List<Expression> markJoinConjuncts, Plan left, Plan right,
            JoinReorderContext otherJoinReorderContext) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), ImmutableList.of(left, right), otherJoinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withJoinType(JoinType joinType) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                groupExpression, Optional.of(getLogicalProperties()), children, joinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withJoinTypeAndContext(JoinType joinType,
            JoinReorderContext otherJoinReorderContext) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), children, otherJoinReorderContext);
    }

    public LogicalJoin<Plan, Plan> withTypeChildren(JoinType joinType, Plan left, Plan right,
            JoinReorderContext otherJoinReorderContext) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts,
                hint, markJoinSlotReference, exceptAsteriskOutputs,
                Optional.empty(), Optional.empty(), ImmutableList.of(left, right), otherJoinReorderContext);
    }

    /**
     * extractNullRejectHashKeys
     */
    public @Nullable Pair<Set<Slot>, Set<Slot>> extractNullRejectHashKeys() {
        // this function is only used by computeFuncDeps, and function dependence calculation is disabled for mark join
        // so markJoinConjuncts is not processed now
        Set<Slot> leftKeys = new HashSet<>();
        Set<Slot> rightKeys = new HashSet<>();
        for (Expression expression : hashJoinConjuncts) {
            // Note we don't support null-safe predicate right now, because we just check uniqueness for join keys
            if (!(expression instanceof EqualTo
                    && ((EqualTo) expression).left() instanceof Slot
                    && ((EqualTo) expression).right() instanceof Slot)) {
                return null;
            }
            Slot leftKey = (Slot) ((EqualTo) expression).left();
            Slot rightKey = (Slot) ((EqualTo) expression).right();
            if (left().getOutputSet().contains(leftKey)) {
                leftKeys.add(leftKey);
                rightKeys.add(rightKey);
            } else {
                leftKeys.add(rightKey);
                rightKeys.add(leftKey);
            }
        }
        return Pair.of(leftKeys, rightKeys);
    }

    /**
     * get Equal slot from join
     */
    public ImmutableEqualSet<Slot> getEqualSlots() {
        // this function is only used by EliminateJoinByFK rule, and EliminateJoinByFK is disabled for mark join
        // so markJoinConjuncts is not processed now
        // TODO: Use fd in the future
        if (!joinType.isInnerJoin() && !joinType.isSemiJoin()) {
            return ImmutableEqualSet.empty();
        }
        ImmutableEqualSet.Builder<Slot> builder = new ImmutableEqualSet.Builder<>();
        hashJoinConjuncts.stream()
                .filter(e -> e instanceof EqualPredicate
                        && e.child(0) instanceof Slot
                        && e.child(1) instanceof Slot)
                .forEach(e ->
                        builder.addEqualPair((Slot) e.child(0), (Slot) e.child(1)));
        return builder.build();
    }

    @Override
    public JSONObject toJson() {
        JSONObject logicalJoin = super.toJson();
        JSONObject properties = new JSONObject();
        properties.put("JoinType", joinType.toString());
        properties.put("HashJoinConjuncts", hashJoinConjuncts.toString());
        properties.put("OtherJoinConjuncts", otherJoinConjuncts.toString());
        properties.put("MarkJoinConjuncts", markJoinConjuncts.toString());
        properties.put("DistributeHint", hint.toString());
        properties.put("MarkJoinSlotReference", markJoinSlotReference.toString());
        logicalJoin.put("Properties", properties);
        return logicalJoin;
    }

    @Override
    public void computeUnique(Builder builder) {
        if (isMarkJoin()) {
            // TODO disable function dependence calculation for mark join, but need re-think this in future.
            return;
        }
        if (joinType.isLeftSemiOrAntiJoin()) {
            builder.addUniqueSlot(left().getLogicalProperties().getTrait());
        } else if (joinType.isRightSemiOrAntiJoin()) {
            builder.addUniqueSlot(right().getLogicalProperties().getTrait());
        }
        // if there is non-equal join conditions, don't propagate unique
        if (hashJoinConjuncts.isEmpty()) {
            return;
        }
        Pair<Set<Slot>, Set<Slot>> keys = extractNullRejectHashKeys();
        if (keys == null) {
            return;
        }

        // Note here we only check whether the left is unique.
        // So the hash condition can't be null-safe
        // TODO: consider Null-safe hash condition when left and rigth is not nullable
        boolean isLeftUnique = left().getLogicalProperties()
                .getTrait().isUnique(keys.first);
        boolean isRightUnique = right().getLogicalProperties()
                .getTrait().isUnique(keys.second);

        // left/right outer join propagate left/right uniforms slots
        // And if the right/left hash keys is unique,
        // join can propagate left/right functional dependencies
        if (joinType.isLeftOuterJoin() && isRightUnique) {
            builder.addUniqueSlot(left().getLogicalProperties().getTrait());
        } else if (joinType.isRightOuterJoin() && isLeftUnique) {
            builder.addUniqueSlot(right().getLogicalProperties().getTrait());
        } else if (joinType.isInnerJoin() && isLeftUnique && isRightUnique) {
            // inner join propagate uniforms slots
            // And if the hash keys is unique, inner join can propagate all functional dependencies
            builder.addDataTrait(left().getLogicalProperties().getTrait());
            builder.addDataTrait(right().getLogicalProperties().getTrait());
        }
    }

    @Override
    public void computeUniform(Builder builder) {
        if (isMarkJoin()) {
            // TODO disable function dependence calculation for mark join, but need re-think this in future.
            return;
        }
        switch (joinType) {
            case INNER_JOIN:
            case CROSS_JOIN:
                builder.addUniformSlot(left().getLogicalProperties().getTrait());
                builder.addUniformSlot(right().getLogicalProperties().getTrait());
                break;
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                builder.addUniformSlot(left().getLogicalProperties().getTrait());
                break;
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                builder.addUniformSlot(right().getLogicalProperties().getTrait());
                break;
            case LEFT_OUTER_JOIN:
                builder.addUniformSlot(left().getLogicalProperties().getTrait());
                builder.addUniformSlotForOuterJoinNullableSide(right().getLogicalProperties().getTrait());
                break;
            case RIGHT_OUTER_JOIN:
                builder.addUniformSlot(right().getLogicalProperties().getTrait());
                builder.addUniformSlotForOuterJoinNullableSide(left().getLogicalProperties().getTrait());
                break;
            case FULL_OUTER_JOIN:
                builder.addUniformSlotForOuterJoinNullableSide(left().getLogicalProperties().getTrait());
                builder.addUniformSlotForOuterJoinNullableSide(right().getLogicalProperties().getTrait());
                break;
            default:
                break;
        }
    }

    @Override
    public void computeEqualSet(Builder builder) {
        if (!joinType.isLeftSemiOrAntiJoin()) {
            builder.addEqualSet(right().getLogicalProperties().getTrait());
        }
        if (!joinType.isRightSemiOrAntiJoin()) {
            builder.addEqualSet(left().getLogicalProperties().getTrait());
        }
        if (joinType.isInnerJoin()) {
            for (Expression expression : getHashJoinConjuncts()) {
                Optional<Pair<Slot, Slot>> equalSlot = ExpressionUtils.extractEqualSlot(expression);
                equalSlot.ifPresent(slotSlotPair -> builder.addEqualPair(slotSlotPair.first, slotSlotPair.second));
            }
        }
    }

    @Override
    public void computeFd(Builder builder) {
        if (!joinType.isLeftSemiOrAntiJoin()) {
            builder.addFuncDepsDG(right().getLogicalProperties().getTrait());
        }
        if (!joinType.isRightSemiOrAntiJoin()) {
            builder.addFuncDepsDG(left().getLogicalProperties().getTrait());
        }
    }

    @Override
    public String getFingerprint() {
        List<Object> args = Lists.newArrayList(
                "type", joinType,
                "hashCondition", hashJoinConjuncts,
                "otherCondition", otherJoinConjuncts,
                "markCondition", markJoinConjuncts);
        return Utils.toSqlString("JOIN", args.toArray());
    }
}
