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

package org.apache.doris.nereids.rules.rewrite.eageraggregation;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.rules.rewrite.MultiJoin;
import org.apache.doris.nereids.rules.rewrite.ReorderJoin;
import org.apache.doris.nereids.rules.rewrite.StatsDerive;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.Statistics;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reorder the inner-join clusters below aggregates before eager aggregation runs.
 *
 * <p>This is a dedicated pre-pass rather than a planner rule. It traverses the plan once, reorders
 * every maximal inner-join cluster at most once, and leaves join trees outside aggregate scopes
 * untouched. The search is a connected greedy search: choose the cheapest connected pair, then
 * repeatedly attach the cheapest predicate-connected atom to form a left-deep tree.</p>
 */
final class EagerAggJoinReorder extends DefaultPlanRewriter<EagerAggJoinReorder.RewriteContext> {
    private final ReorderJoin reorderJoin = new ReorderJoin();
    private final StatsDerive statsDerive = new StatsDerive(false);

    /**
     * Run the pre-pass once before {@link PushDownAggregation} starts its own tree traversal.
     */
    Plan rewrite(Plan root, CascadesContext context) {
        SessionVariable sessionVariable = context.getConnectContext().getSessionVariable();
        if (sessionVariable.isDisableJoinReorder()
                || !sessionVariable.enableInitJoinOrder
                || context.isLeadingDisableJoinReorder()) {
            return root;
        }
        return root.accept(this, new RewriteContext(context, false, false));
    }

    @Override
    public Plan visit(Plan plan, RewriteContext context) {
        return DefaultPlanRewriter.visitChildren(this, plan, context.forPlanChildren(false));
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, RewriteContext context) {
        boolean reorderAggregateChild = !aggregate.getSourceRepeat().isPresent()
                && !aggregate.getGroupByExpressions().isEmpty();
        Plan child = aggregate.child();
        Plan newChild = child.accept(this, context.forAggregateChild(reorderAggregateChild));
        return newChild == child ? aggregate : aggregate.withChildren(Collections.singletonList(newChild));
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, RewriteContext context) {
        if (context.underAggregate && isInnerJoinClusterNode(filter)) {
            return visitJoinClusterNode(filter, context);
        }
        return super.visitLogicalFilter(filter, context);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, RewriteContext context) {
        if (context.underAggregate && join.getJoinType().isInnerOrCrossJoin()) {
            return visitJoinClusterNode(join, context);
        }
        return super.visitLogicalJoin(join, context);
    }

    private Plan visitJoinClusterNode(Plan root, RewriteContext context) {
        Plan current = DefaultPlanRewriter.visitChildren(this, root, context.forPlanChildren(true));
        return context.parentIsJoinCluster
                ? current
                : reorderJoinCluster(current, context.cascadesContext);
    }

    private boolean isInnerJoinClusterNode(Plan root) {
        if (root instanceof LogicalJoin) {
            return ((LogicalJoin<?, ?>) root).getJoinType().isInnerOrCrossJoin();
        }
        return root instanceof LogicalFilter
                && root.child(0) instanceof LogicalJoin
                && ((LogicalJoin<?, ?>) root.child(0)).getJoinType().isInnerOrCrossJoin();
    }

    private Plan reorderJoinCluster(Plan root, CascadesContext context) {
        if (containsUnsupportedJoin(root)) {
            return root;
        }

        Map<Plan, DistributeHint> planToHintType = new HashMap<>();
        Plan multiJoinPlan = reorderJoin.joinToMultiJoin(root, planToHintType);
        if (!(multiJoinPlan instanceof MultiJoin)) {
            return root;
        }

        MultiJoin multiJoin = (MultiJoin) multiJoinPlan;
        int atomCount = multiJoin.arity();
        if (!multiJoin.getJoinType().isInnerOrCrossJoin()
                || atomCount <= 2
                || atomCount > context.getConnectContext().getSessionVariable().getMaxJoinNumberOfReorder()
                // An inner cluster may contain an outer/semi/anti join as an indivisible atom.
                || multiJoin.children().stream().anyMatch(MultiJoin.class::isInstance)) {
            return root;
        }

        List<CatalogRelation> scans = multiJoin.collectToList(CatalogRelation.class::isInstance);
        if (StatsCalculator.disableJoinReorderIfStatsInvalid(scans, context).isPresent()) {
            return root;
        }

        List<Atom> atoms = new ArrayList<>(atomCount);
        for (int i = 0; i < atomCount; i++) {
            Plan atom = multiJoin.child(i);
            double rowCount = deriveRowCount(atom);
            if (!isValidRowCount(rowCount)) {
                return root;
            }
            atoms.add(new Atom(atom, rowCount, i));
        }

        LinkedHashSet<Expression> remainingPredicates = new LinkedHashSet<>(multiJoin.getJoinFilter());
        Candidate current = chooseInitialPair(atoms, remainingPredicates);
        if (current == null) {
            return root;
        }
        remainingPredicates.removeAll(current.consumedPredicates);

        while (current.atoms.cardinality() < atomCount) {
            Candidate best = null;
            for (Atom atom : atoms) {
                if (current.atoms.get(atom.index)) {
                    continue;
                }
                BitSet candidateAtoms = (BitSet) current.atoms.clone();
                candidateAtoms.set(atom.index);
                Candidate candidate = buildJoinCandidate(
                        current.plan, current.cost, current.rowCount,
                        atom.plan, atom.rowCount, atom.rowCount,
                        remainingPredicates, candidateAtoms, atom.index, false);
                if (isBetter(candidate, best)) {
                    best = candidate;
                }
            }
            // Do not introduce a new cross join merely to complete the greedy tree.
            if (best == null) {
                return root;
            }
            current = best;
            remainingPredicates.removeAll(current.consumedPredicates);
        }

        return PlanUtils.filterOrSelf(remainingPredicates, current.plan);
    }

    private Candidate chooseInitialPair(List<Atom> atoms, Set<Expression> predicates) {
        Candidate best = null;
        int atomCount = atoms.size();
        for (int leftIndex = 0; leftIndex < atomCount; leftIndex++) {
            Atom left = atoms.get(leftIndex);
            for (int rightIndex = leftIndex + 1; rightIndex < atomCount; rightIndex++) {
                Atom right = atoms.get(rightIndex);
                BitSet joinedAtoms = new BitSet(atomCount);
                joinedAtoms.set(left.index);
                joinedAtoms.set(right.index);
                Candidate candidate = buildJoinCandidate(
                        left.plan, left.rowCount, left.rowCount,
                        right.plan, right.rowCount, right.rowCount,
                        predicates, joinedAtoms, leftIndex * atomCount + rightIndex, true);
                if (isBetter(candidate, best)) {
                    best = candidate;
                }
            }
        }
        return best;
    }

    private Candidate buildJoinCandidate(Plan left, double leftCost, double leftRows,
            Plan right, double rightCost, double rightRows, Set<Expression> predicates,
            BitSet joinedAtoms, int tieBreaker, boolean putLargerAtomOnLeft) {
        if (putLargerAtomOnLeft && leftRows < rightRows) {
            Plan oldLeft = left;
            left = right;
            right = oldLeft;

            double oldLeftCost = leftCost;
            leftCost = rightCost;
            rightCost = oldLeftCost;

            double oldLeftRows = leftRows;
            leftRows = rightRows;
            rightRows = oldLeftRows;
        }

        Set<ExprId> leftOutputExprIds = left.getOutputExprIdSet();
        Set<ExprId> rightOutputExprIds = right.getOutputExprIdSet();
        Set<ExprId> joinOutputExprIds = JoinUtils.getJoinOutputExprIdSet(left, right);
        List<Expression> joinPredicates = predicates.stream()
                .filter(predicate -> {
                    Set<ExprId> inputExprIds = predicate.getInputSlotExprIds();
                    return !leftOutputExprIds.containsAll(inputExprIds)
                            && !rightOutputExprIds.containsAll(inputExprIds)
                            && joinOutputExprIds.containsAll(inputExprIds);
                })
                .collect(Collectors.toList());
        if (joinPredicates.isEmpty()) {
            return null;
        }

        Pair<List<Expression>, List<Expression>> conditions = JoinUtils.extractExpressionForHashTable(
                left.getOutput(), right.getOutput(), joinPredicates);
        LogicalJoin<Plan, Plan> join = new LogicalJoin<>(
                JoinType.INNER_JOIN,
                conditions.first,
                conditions.second,
                new DistributeHint(DistributeType.NONE),
                Optional.empty(),
                left,
                right,
                JoinReorderContext.EMPTY);
        double outputRows = deriveRowCount(join);
        if (!isValidRowCount(outputRows)) {
            return null;
        }

        double cost = saturatingAdd(outputRows, saturatingAdd(leftCost, rightCost));
        return new Candidate(join, joinedAtoms, joinPredicates, outputRows, cost, rightRows, tieBreaker);
    }

    private double deriveRowCount(Plan plan) {
        Statistics statistics = ((AbstractPlan) plan).getStats();
        if (statistics == null) {
            statistics = plan.accept(statsDerive, new DeriveContext());
        }
        return statistics == null ? Double.NaN : statistics.getRowCount();
    }

    private boolean containsUnsupportedJoin(Plan root) {
        if (root instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) root;
            if (filter.getConjuncts().stream().anyMatch(Expression::containsVolatileExpression)) {
                return true;
            }
            return containsUnsupportedJoin(filter.child());
        }
        if (!(root instanceof LogicalJoin)) {
            return false;
        }

        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) root;
        if (!join.getJoinType().isInnerOrCrossJoin()) {
            return false;
        }
        DistributeHint hint = join.getDistributeHint();
        if (join.isMarkJoin() || join.getJoinType().isAsofJoin() || join.isLeadingJoin()
                || hint.distributeType != DistributeType.NONE
                || join.getExpressions().stream().anyMatch(Expression::containsVolatileExpression)) {
            return true;
        }
        return containsUnsupportedJoin(join.left()) || containsUnsupportedJoin(join.right());
    }

    private boolean isBetter(Candidate candidate, Candidate currentBest) {
        if (candidate == null) {
            return false;
        }
        if (currentBest == null) {
            return true;
        }
        int result = Double.compare(candidate.cost, currentBest.cost);
        if (result != 0) {
            return result < 0;
        }
        result = Double.compare(candidate.rowCount, currentBest.rowCount);
        if (result != 0) {
            return result < 0;
        }
        result = Double.compare(candidate.rightRowCount, currentBest.rightRowCount);
        if (result != 0) {
            return result < 0;
        }
        return candidate.tieBreaker < currentBest.tieBreaker;
    }

    private boolean isValidRowCount(double rowCount) {
        return Double.isFinite(rowCount) && rowCount >= 0;
    }

    private double saturatingAdd(double left, double right) {
        return left > Double.MAX_VALUE - right ? Double.MAX_VALUE : left + right;
    }

    private static class Atom {
        private final Plan plan;
        private final double rowCount;
        private final int index;

        private Atom(Plan plan, double rowCount, int index) {
            this.plan = plan;
            this.rowCount = rowCount;
            this.index = index;
        }
    }

    static class RewriteContext {
        private final CascadesContext cascadesContext;
        private final boolean underAggregate;
        private final boolean parentIsJoinCluster;

        private RewriteContext(CascadesContext cascadesContext, boolean underAggregate,
                boolean parentIsJoinCluster) {
            this.cascadesContext = cascadesContext;
            this.underAggregate = underAggregate;
            this.parentIsJoinCluster = parentIsJoinCluster;
        }

        private RewriteContext forAggregateChild(boolean reorderAggregateChild) {
            return new RewriteContext(cascadesContext, reorderAggregateChild, false);
        }

        private RewriteContext forPlanChildren(boolean parentIsJoinCluster) {
            return new RewriteContext(cascadesContext, underAggregate, parentIsJoinCluster);
        }
    }

    private static class Candidate {
        private final Plan plan;
        private final BitSet atoms;
        private final List<Expression> consumedPredicates;
        private final double rowCount;
        private final double cost;
        private final double rightRowCount;
        private final int tieBreaker;

        private Candidate(Plan plan, BitSet atoms, List<Expression> consumedPredicates,
                double rowCount, double cost, double rightRowCount, int tieBreaker) {
            this.plan = plan;
            this.atoms = atoms;
            this.consumedPredicates = consumedPredicates;
            this.rowCount = rowCount;
            this.cost = cost;
            this.rightRowCount = rightRowCount;
            this.tieBreaker = tieBreaker;
        }
    }
}
