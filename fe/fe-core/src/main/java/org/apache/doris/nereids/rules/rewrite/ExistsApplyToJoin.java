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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Optional;

/**
 * Convert ExistsApply to LogicalJoin.
 * <p>
 * Exists
 *    Correlated -> LEFT_SEMI_JOIN
 *         apply                  LEFT_SEMI_JOIN(Correlated Predicate)
 *      /       \         -->       /           \
 *    input    queryPlan          input        queryPlan
 * <p>
 *    UnCorrelated -> CROSS_JOIN(limit(1))
 *          apply                       CROSS_JOIN
 *      /           \          -->      /       \
 *    input        queryPlan          input    limit(1)
 *                                               |
 *                                             queryPlan
 * <p>
 * Not Exists
 *    Correlated -> LEFT_ANTI_JOIN
 *          apply                  LEFT_ANTI_JOIN(Correlated Predicate)
 *       /       \         -->       /           \
 *     input    queryPlan          input        queryPlan
 * <p>
 *    UnCorrelated -> CROSS_JOIN(Count(*))
 *                                    Filter(count(*) = 0)
 *                                          |
 *         apply                       Cross_Join
 *      /       \         -->       /           \
 *    input    queryPlan          input       agg(output:count(*))
 *                                               |
 *                                             limit(1)
 *                                               |
 *                                             queryPlan
 */
public class ExistsApplyToJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalApply().when(LogicalApply::isExist).then(apply -> {
            // apply.isCorrelated() only check if correlated slot exits
            // but correlation filter may be eliminated by SimplifyConflictCompound rule
            // so we need check both correlated slot and correlation filter exists before creating LogicalJoin node
            if (apply.isCorrelated() && apply.getCorrelationFilter().isPresent()) {
                return correlatedToJoin(apply);
            } else {
                return unCorrelatedToJoin(apply);
            }
        }).toRule(RuleType.EXISTS_APPLY_TO_JOIN);
    }

    private Plan correlatedToJoin(LogicalApply<?, ?> apply) {
        Optional<Expression> correlationFilter = apply.getCorrelationFilter();
        if (apply.isNot()) {
            return new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN, ExpressionUtils.EMPTY_CONDITION,
                    correlationFilter.map(ExpressionUtils::extractConjunction).orElse(ExpressionUtils.EMPTY_CONDITION),
                    new DistributeHint(DistributeType.NONE),
                    apply.getMarkJoinSlotReference(),
                    apply.children(), null);
        } else {
            return new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN, ExpressionUtils.EMPTY_CONDITION,
                    correlationFilter.map(ExpressionUtils::extractConjunction).orElse(ExpressionUtils.EMPTY_CONDITION),
                    new DistributeHint(DistributeType.NONE),
                    apply.getMarkJoinSlotReference(),
                    apply.children(), null);
        }
    }

    private Plan unCorrelatedToJoin(LogicalApply<?, ?> unapply) {
        if (unapply.isNot()) {
            return unCorrelatedNotExist(unapply);
        } else {
            return unCorrelatedExist(unapply);
        }
    }

    private Plan unCorrelatedNotExist(LogicalApply<?, ?> unapply) {
        LogicalLimit<?> newLimit = new LogicalLimit<>(1, 0, LimitPhase.ORIGIN, (LogicalPlan) unapply.right());
        Alias alias = new Alias(new Count(), "count(*)");
        LogicalAggregate<?> newAgg = new LogicalAggregate<>(new ArrayList<>(),
                ImmutableList.of(alias), newLimit);
        LogicalJoin<?, ?> newJoin = new LogicalJoin<>(JoinType.CROSS_JOIN, ExpressionUtils.EMPTY_CONDITION,
                ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE),
                unapply.getMarkJoinSlotReference(),
                (LogicalPlan) unapply.left(), newAgg, null);
        return new LogicalFilter<>(ImmutableSet.of(new EqualTo(newAgg.getOutput().get(0),
                new IntegerLiteral(0))), newJoin);
    }

    private Plan unCorrelatedExist(LogicalApply<?, ?> unapply) {
        LogicalLimit<?> newLimit = new LogicalLimit<>(1, 0, LimitPhase.ORIGIN, (LogicalPlan) unapply.right());
        return new LogicalJoin<>(JoinType.CROSS_JOIN, ExpressionUtils.EMPTY_CONDITION,
            ExpressionUtils.EMPTY_CONDITION,
                new DistributeHint(DistributeType.NONE), unapply.getMarkJoinSlotReference(),
                (LogicalPlan) unapply.left(), newLimit, null);
    }
}
