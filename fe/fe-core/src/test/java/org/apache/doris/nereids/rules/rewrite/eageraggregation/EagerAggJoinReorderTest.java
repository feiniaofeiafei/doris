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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.StatisticsBuilder;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class EagerAggJoinReorderTest {

    @Test
    void testChooseSmallSelectiveDimensionBeforeCustomer() {
        Fixture fixture = createStarJoinFixture(100, 10_000);
        Plan reordered = rewrite(fixture, createContext(fixture.aggregate));

        Assertions.assertEquals(
                ImmutableList.of(fixture.storeSales, fixture.dateDim, fixture.customer),
                collectRelations(((LogicalAggregate<?>) reordered).child()));
    }

    @Test
    void testChooseMoreSelectiveCustomerWhenItsFilteredRowsAreSmaller() {
        Fixture fixture = createStarJoinFixture(100, 10);
        Plan reordered = rewrite(fixture, createContext(fixture.aggregate));

        Assertions.assertEquals(
                ImmutableList.of(fixture.storeSales, fixture.customer, fixture.dateDim),
                collectRelations(((LogicalAggregate<?>) reordered).child()));
    }

    @Test
    void testOnlyReorderJoinBelowAggregate() {
        Fixture fixture = createStarJoinFixture(100, 10_000);
        CascadesContext context = createContext(fixture.aggregate);

        Assertions.assertSame(fixture.topJoin,
                new EagerAggJoinReorder().rewrite(fixture.topJoin, context));
        Assertions.assertNotSame(fixture.aggregate,
                new EagerAggJoinReorder().rewrite(fixture.aggregate, context));
    }

    @Test
    void testRespectJoinReorderSwitchAndDistributionHint() {
        Fixture fixture = createStarJoinFixture(100, 10_000);
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        CascadesContext context = MemoTestUtils.createCascadesContext(connectContext, fixture.aggregate);

        connectContext.getSessionVariable().setDisableJoinReorder(true);
        Assertions.assertSame(fixture.aggregate,
                new EagerAggJoinReorder().rewrite(fixture.aggregate, context));

        connectContext.getSessionVariable().setDisableJoinReorder(false);
        fixture.topJoin.setHint(new DistributeHint(DistributeType.BROADCAST_RIGHT));
        Assertions.assertSame(fixture.aggregate,
                new EagerAggJoinReorder().rewrite(fixture.aggregate, context));
    }

    @Test
    void testKeepOriginalPlanWhenStatisticsAreInvalid() {
        Fixture fixture = createStarJoinFixture(Double.NaN, 10_000);

        Assertions.assertSame(fixture.aggregate,
                rewrite(fixture, createContext(fixture.aggregate)));
    }

    private Plan rewrite(Fixture fixture, CascadesContext context) {
        return new EagerAggJoinReorder().rewrite(fixture.aggregate, context);
    }

    private CascadesContext createContext(Plan root) {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        connectContext.getSessionVariable().setDisableJoinReorder(false);
        return MemoTestUtils.createCascadesContext(connectContext, root);
    }

    private List<LogicalOneRowRelation> collectRelations(Plan root) {
        return root.collectToList(LogicalOneRowRelation.class::isInstance);
    }

    private Fixture createStarJoinFixture(double dateRows, double customerRows) {
        LogicalOneRowRelation storeSales = relationWithStats("store_sales", 1_000_000);
        LogicalOneRowRelation dateDim = relationWithStats("date_dim", dateRows);
        LogicalOneRowRelation customer = relationWithStats("customer", customerRows);

        LogicalJoin<Plan, Plan> customerJoinSales = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(customer.getOutput().get(0), storeSales.getOutput().get(0))),
                customer, storeSales, JoinReorderContext.EMPTY);
        LogicalJoin<Plan, Plan> topJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(storeSales.getOutput().get(0), dateDim.getOutput().get(0))),
                customerJoinSales, dateDim, JoinReorderContext.EMPTY);
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(storeSales.getOutput().get(0)),
                ImmutableList.of(storeSales.getOutput().get(0)),
                topJoin);
        return new Fixture(storeSales, dateDim, customer, topJoin, aggregate);
    }

    private LogicalOneRowRelation relationWithStats(String name, double rowCount) {
        LogicalOneRowRelation relation = new LogicalOneRowRelation(
                StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of(new Alias(new IntegerLiteral(1), name)));
        ((AbstractPlan) relation).setStatistics(new StatisticsBuilder().setRowCount(rowCount).build());
        return relation;
    }

    private static class Fixture {
        private final LogicalOneRowRelation storeSales;
        private final LogicalOneRowRelation dateDim;
        private final LogicalOneRowRelation customer;
        private final LogicalJoin<Plan, Plan> topJoin;
        private final LogicalAggregate<Plan> aggregate;

        private Fixture(LogicalOneRowRelation storeSales, LogicalOneRowRelation dateDim,
                LogicalOneRowRelation customer, LogicalJoin<Plan, Plan> topJoin,
                LogicalAggregate<Plan> aggregate) {
            this.storeSales = storeSales;
            this.dateDim = dateDim;
            this.customer = customer;
            this.topJoin = topJoin;
            this.aggregate = aggregate;
        }
    }
}
