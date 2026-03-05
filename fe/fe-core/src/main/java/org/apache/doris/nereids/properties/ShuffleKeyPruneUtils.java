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

package org.apache.doris.nereids.properties;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**AggShuffleKeyOptimize*/
public class ShuffleKeyPruneUtils {
    private static GroupExpression getGroupExpression(Group group) {
        List<GroupExpression> physicalGroupExpressions = group.getPhysicalExpressions();
        if (!physicalGroupExpressions.isEmpty()) {
            return physicalGroupExpressions.get(0);
        } else {
            return group.getLogicalExpressions().get(0);
        }
    }

    /*
     * @param agg is a global aggregate
     * @return the Statistics of the children of the local aggregate corresponding to the global aggregate.
     */
    private static Optional<Statistics> getGlobalAggChildStats(PhysicalHashAggregate<? extends Plan> agg) {
        Optional<GroupExpression> groupExpression = agg.getGroupExpression();
        if (!groupExpression.isPresent()) {
            return Optional.empty();
        }
        Statistics aggChildStats = groupExpression.get().childStatistics(0);
        Group childGroup = groupExpression.get().child(0);
        Plan childExpression = getGroupExpression(childGroup).getPlan();
        if (childExpression instanceof PhysicalHashAggregate
                && ((PhysicalHashAggregate) childExpression).getAggPhase().isLocal()) {
            childGroup = childGroup.getPhysicalExpressions().get(0).child(0);
            aggChildStats = childGroup.getStatistics();
        }
        return Optional.ofNullable(aggChildStats);
    }

    private static boolean canAggShuffleKeyOpt(PhysicalHashAggregate<? extends Plan> agg,
            List<? extends Expression> partitionExprs, ConnectContext connectContext) {
        if (!connectContext.getSessionVariable().enableAggShuffleKeyPrune) {
            return false;
        }
        if (partitionExprs.size() <= connectContext.getSessionVariable().shuffleKeyPruneThreshold) {
            return false;
        }
        if (agg.hasSourceRepeat()) {
            return false;
        }
        return true;
    }

    /**
     * When parent sends shuffle request, choose one optimal key from intersection of parent hash
     * columns and agg group-by columns, or use full intersection. Returns list of ExprIds as
     * shuffle keys.
     */
    public static List<ExprId> selectOptimalShuffleKeyForAggWithParentHashRequest(
            PhysicalHashAggregate<? extends Plan> agg, Set<ExprId> intersectIdSet, PlanContext context) {
        List<ExprId> orderedIds = Utils.fastToImmutableList(intersectIdSet);
        if (!context.getConnectContext().getSessionVariable().enableAggShuffleKeyPrune
                || intersectIdSet.size() <= context.getConnectContext().getSessionVariable().shuffleKeyPruneThreshold) {
            return orderedIds;
        }
        Optional<Statistics> childStats = getGlobalAggChildStats(agg);
        if (!childStats.isPresent()) {
            return orderedIds;
        }
        List<Expression> intersectExprs = new ArrayList<>();
        for (Expression e : agg.getGroupByExpressions()) {
            if (e instanceof SlotReference) {
                SlotReference slot = (SlotReference) e;
                if (intersectIdSet.contains(slot.getExprId())) {
                    intersectExprs.add(e);
                }
            }
        }
        if (intersectExprs.isEmpty()) {
            return orderedIds;
        }
        double rowCount = childStats.get().getRowCount();
        int instanceNum = ConnectContext.getTotalInstanceNum(context.getConnectContext());
        Optional<List<Expression>> optimalKeys = selectOptimalShuffleKeys(
                intersectExprs, childStats.get(), rowCount, instanceNum);
        if (optimalKeys.isPresent()) {
            return optimalKeys.get().stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .map(SlotReference::getExprId)
                    .collect(Collectors.toList());
        }
        return orderedIds;
    }

    /**
     * Scenario 4: When partition expressions are set by rule, optionally reduce shuffle keys.
     * Strategy: 1) Try single key (isBalanced); 2) Try numeric+date keys (remove strings);
     * 3) Fall back to full partitionExprs.
     * Returns the list of expressions to use as shuffle keys, or empty to use full partitionExprs.
     */
    public static Optional<List<Expression>> selectBestShuffleKeyForAgg(
            PhysicalHashAggregate<? extends Plan> agg, List<Expression> partitionExprs, ConnectContext context) {
        if (!canAggShuffleKeyOpt(agg, partitionExprs, context)) {
            return Optional.empty();
        }
        Optional<Statistics> childStats = getGlobalAggChildStats(agg);
        if (!childStats.isPresent()) {
            return Optional.empty();
        }
        double rowCount = childStats.get().getRowCount();
        int instanceNum = ConnectContext.getTotalInstanceNum(context);
        return selectOptimalShuffleKeys(partitionExprs, childStats.get(), rowCount, instanceNum);
    }

    /**
     * Select optimal shuffle keys with three-step strategy:
     * 1. Try single key: sort by type (numeric/date first, string sorted by avg_size), pick first isBalanced key.
     * 2. Try remove strings: filter numeric+date keys, if combinedNDV > instanceNum*512 return that list.
     * 3. Fall back: return empty (caller uses full partitionExprs).
     */
    private static Optional<List<Expression>> selectOptimalShuffleKeys(List<Expression> partitionExprs,
            Statistics childStats, double rowCount, int instanceNum) {
        List<SlotReference> slotRefs = partitionExprs.stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .collect(Collectors.toList());
        if (slotRefs.isEmpty()) {
            return Optional.empty();
        }
        // If any partition slot lacks column stats, skip optimization and use original partitionExprs.
        for (SlotReference slotRef : slotRefs) {
            if (childStats.findColumnStatistics(slotRef) == null) {
                return Optional.empty();
            }
        }

        // Step 1: Try single key - sort by type priority, pick first isBalanced
        List<SlotReference> sortedByType = sortShuffleKeysByTypePriority(slotRefs, childStats);
        for (SlotReference slotRef : sortedByType) {
            ColumnStatistic colStats = childStats.findColumnStatistics(slotRef);
            if (StatisticsUtil.isBalanced(colStats, rowCount, instanceNum)) {
                return Optional.of(ImmutableList.of(slotRef));
            }
        }

        // Step 2: Try remove string types - filter numeric+date, check combined NDV
        List<Expression> numericAndDateExprs = slotRefs.stream()
                .filter(s -> s.getDataType().isNumericType() || s.getDataType().isDateLikeType())
                .collect(Collectors.toList());
        if (!numericAndDateExprs.isEmpty()) {
            double combinedNdv = StatsCalculator.estimateGroupByRowCount(numericAndDateExprs, childStats);
            long ndvThreshold = (long) instanceNum * AggregateUtils.NDV_INSTANCE_BALANCE_MULTIPLIER;
            if (combinedNdv > ndvThreshold) {
                return Optional.of(ImmutableList.copyOf(numericAndDateExprs));
            }
        }

        // Step 3: Fall back - return empty, caller uses full partitionExprs
        return Optional.empty();
    }

    /**
     * Sort shuffle keys: numeric and date first, then string types.
     * String types are sorted by column statistics avg size (avgSizeByte) ascending.
     */
    private static List<SlotReference> sortShuffleKeysByTypePriority(List<SlotReference> slotRefs,
            Statistics childStats) {
        List<SlotReference> result = new ArrayList<>(slotRefs);
        result.sort(Comparator
                .comparingInt((SlotReference s) -> getTypeSortPriority(s.getDataType()))
                .thenComparingDouble((SlotReference s) -> getStringAvgSizeForSort(s, childStats)));
        return result;
    }

    /** 0=numeric/date first, 1=string last. */
    private static int getTypeSortPriority(DataType dataType) {
        if (dataType.isNumericType() || dataType.isDateLikeType()) {
            return 0;
        }
        return 1;
    }

    /** For string types return avg size from stats; for others return 0 (no secondary sort). */
    private static double getStringAvgSizeForSort(SlotReference slotRef, Statistics childStats) {
        DataType dataType = slotRef.getDataType();
        if (dataType instanceof CharacterType) {
            ColumnStatistic colStats = childStats.findColumnStatistics(slotRef);
            if (colStats != null && !colStats.isUnKnown && colStats.avgSizeByte > 0) {
                return colStats.avgSizeByte;
            }
            return ((CharacterType) dataType).getLen();
        }
        return 0;
    }

    /**
     * Get Global AGG plan and its input statistics from a Group (if the group's best plan is Global AGG).
     */
    private static Optional<Pair<PhysicalHashAggregate<? extends Plan>, Statistics>> getGlobalAggInputStatsFromGroup(
            Group group) {
        for (GroupExpression ge : group.getPhysicalExpressions()) {
            Plan p = ge.getPlan();
            if (p instanceof PhysicalHashAggregate && ((PhysicalHashAggregate<?>) p).getAggPhase().isGlobal()) {
                Optional<Statistics> inputStats = getGlobalAggChildStats((PhysicalHashAggregate<? extends Plan>) p);
                return inputStats.map(statistics -> Pair.of((PhysicalHashAggregate<? extends Plan>) p, statistics));
            }
        }
        return Optional.empty();
    }

    /**
     * Scenario 3.3: when both join children are Global AGG, find optimal shuffle keys from
     * join key ∩ left_agg.gby ∩ right_agg.gby. Same three-step strategy as agg:
     * 1) Try single key (isBalanced); 2) Try numeric+date keys (remove strings);
     * 3) Fall back. Returns (leftKeys, rightKeys) or empty.
     */
    public static Optional<Pair<List<ExprId>, List<ExprId>>> tryFindOptimalShuffleKeyForBothAggChildren(
            PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, PlanContext context) {
        if (hashJoin.getHashJoinConjuncts().size()
                <= context.getConnectContext().getSessionVariable().shuffleKeyPruneThreshold) {
            return Optional.empty();
        }
        GroupExpression joinGroupExpr = context.getGroupExpression();
        if (joinGroupExpr == null) {
            return Optional.empty();
        }
        Group leftGroup = joinGroupExpr.child(0);
        Group rightGroup = joinGroupExpr.child(1);
        Optional<Pair<PhysicalHashAggregate<? extends Plan>, Statistics>> leftOpt =
                getGlobalAggInputStatsFromGroup(leftGroup);
        Optional<Pair<PhysicalHashAggregate<? extends Plan>, Statistics>> rightOpt =
                getGlobalAggInputStatsFromGroup(rightGroup);
        if (!leftOpt.isPresent() || !rightOpt.isPresent()) {
            return Optional.empty();
        }

        PhysicalHashAggregate<? extends Plan> leftAgg = leftOpt.get().first;
        PhysicalHashAggregate<? extends Plan> rightAgg = rightOpt.get().first;
        if (leftAgg.hasSourceRepeat() || rightAgg.hasSourceRepeat()) {
            return Optional.empty();
        }
        Statistics leftStats = leftOpt.get().second;
        Statistics rightStats = rightOpt.get().second;

        Pair<List<ExprId>, List<ExprId>> joinKeys = hashJoin.getHashConjunctsExprIds();
        if (joinKeys.first.isEmpty() || joinKeys.second.size() != joinKeys.first.size()) {
            return Optional.empty();
        }

        Set<ExprId> leftGbyIds = leftAgg.getGroupByExpressions().stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .map(SlotReference::getExprId)
                .collect(Collectors.toSet());
        Set<ExprId> rightGbyIds = rightAgg.getGroupByExpressions().stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .map(SlotReference::getExprId)
                .collect(Collectors.toSet());

        double leftRows = leftStats.getRowCount();
        double rightRows = rightStats.getRowCount();
        int instanceNum = ConnectContext.getTotalInstanceNum(context.getConnectContext());

        // Build (leftSlotRef, rightSlotRef) pairs for join keys in both gby sets
        List<Pair<SlotReference, SlotReference>> validPairs = new ArrayList<>();
        for (int i = 0; i < joinKeys.first.size(); i++) {
            ExprId leftId = joinKeys.first.get(i);
            ExprId rightId = joinKeys.second.get(i);
            if (!leftGbyIds.contains(leftId) || !rightGbyIds.contains(rightId)) {
                continue;
            }
            SlotReference leftSlotRef = leftAgg.getGroupByExpressions().stream()
                    .filter(e -> e instanceof SlotReference && ((SlotReference) e).getExprId().equals(leftId))
                    .map(SlotReference.class::cast)
                    .findFirst()
                    .orElse(null);
            SlotReference rightSlotRef = rightAgg.getGroupByExpressions().stream()
                    .filter(e -> e instanceof SlotReference && ((SlotReference) e).getExprId().equals(rightId))
                    .map(SlotReference.class::cast)
                    .findFirst()
                    .orElse(null);
            if (leftSlotRef != null && rightSlotRef != null) {
                validPairs.add(Pair.of(leftSlotRef, rightSlotRef));
            }
        }
        if (validPairs.isEmpty()) {
            return Optional.empty();
        }
        // If any join key pair lacks column stats on either side, skip optimization.
        for (Pair<SlotReference, SlotReference> pair : validPairs) {
            if (leftStats.findColumnStatistics(pair.first) == null
                    || rightStats.findColumnStatistics(pair.second) == null) {
                return Optional.empty();
            }
        }

        // Step 1: Try single key - sort by type, pick first where both isBalanced
        List<Pair<SlotReference, SlotReference>> sortedPairs =
                sortJoinKeyPairsByTypePriority(validPairs, leftStats, rightStats);
        for (Pair<SlotReference, SlotReference> pair : sortedPairs) {
            SlotReference leftSlotRef = pair.first;
            SlotReference rightSlotRef = pair.second;
            ColumnStatistic leftColStats = leftStats.findColumnStatistics(leftSlotRef);
            ColumnStatistic rightColStats = rightStats.findColumnStatistics(rightSlotRef);
            if (StatisticsUtil.isBalanced(leftColStats, leftRows, instanceNum)
                    && StatisticsUtil.isBalanced(rightColStats, rightRows, instanceNum)) {
                return Optional.of(Pair.of(
                        ImmutableList.of(leftSlotRef.getExprId()),
                        ImmutableList.of(rightSlotRef.getExprId())));
            }
        }

        // Step 2: Try remove string types - filter numeric+date pairs, check combined NDV
        List<SlotReference> numericDateLeftSlots = new ArrayList<>();
        List<SlotReference> numericDateRightSlots = new ArrayList<>();
        for (Pair<SlotReference, SlotReference> pair : validPairs) {
            if ((pair.first.getDataType().isNumericType() || pair.first.getDataType().isDateLikeType())
                    && (pair.second.getDataType().isNumericType() || pair.second.getDataType().isDateLikeType())) {
                numericDateLeftSlots.add(pair.first);
                numericDateRightSlots.add(pair.second);
            }
        }
        if (!numericDateLeftSlots.isEmpty()) {
            double leftCombinedNdv = StatsCalculator.estimateGroupByRowCount(
                    new ArrayList<>(numericDateLeftSlots), leftStats);
            double rightCombinedNdv = StatsCalculator.estimateGroupByRowCount(
                    new ArrayList<>(numericDateRightSlots), rightStats);
            long ndvThreshold = (long) instanceNum * AggregateUtils.NDV_INSTANCE_BALANCE_MULTIPLIER;
            if (leftCombinedNdv > ndvThreshold && rightCombinedNdv > ndvThreshold) {
                List<ExprId> leftIds = numericDateLeftSlots.stream()
                        .map(SlotReference::getExprId)
                        .collect(Collectors.toList());
                List<ExprId> rightIds = numericDateRightSlots.stream()
                        .map(SlotReference::getExprId)
                        .collect(Collectors.toList());
                return Optional.of(Pair.of(leftIds, rightIds));
            }
        }

        // Step 3: Fall back
        return Optional.empty();
    }

    /** Sort join key pairs by type priority (numeric/date first, string by avg_size). */
    private static List<Pair<SlotReference, SlotReference>> sortJoinKeyPairsByTypePriority(
            List<Pair<SlotReference, SlotReference>> pairs, Statistics leftStats, Statistics rightStats) {
        List<Pair<SlotReference, SlotReference>> result = new ArrayList<>(pairs);
        result.sort(Comparator
                .comparingInt((Pair<SlotReference, SlotReference> p) ->
                        getTypeSortPriority(p.first.getDataType()))
                .thenComparingDouble((Pair<SlotReference, SlotReference> p) ->
                        getJoinPairStringAvgSizeForSort(p, leftStats, rightStats)));
        return result;
    }

    /** For string join-key pairs, use avg size of both sides for sorting; for others return 0. */
    private static double getJoinPairStringAvgSizeForSort(Pair<SlotReference, SlotReference> pair,
            Statistics leftStats, Statistics rightStats) {
        if (pair.first.getDataType() instanceof CharacterType && pair.second.getDataType() instanceof CharacterType) {
            return (getStringAvgSizeForSort(pair.first, leftStats) + getStringAvgSizeForSort(pair.second, rightStats));
        }
        return 0;
    }
}
