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

package org.apache.doris.catalog;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.commands.AlterViewCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateViewCommand;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.ViewSqlRewriter;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ViewDefinitionBindingTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("view_binding");
        useDatabase("view_binding");
        createTable("create table binding_source(k int, v int, payload variant, items array<struct<value:int>>)"
                + " duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num' = '1')");
    }

    @Test
    public void testRepeatedReferencesHaveIndependentSourcePositions() throws Exception {
        createView("create view repeated_references as select s.k as first_k, s.k + 1 as next_k"
                + " from binding_source s where s.k > 0 and s.k < 10");

        View view = assertDefinitionAnalyzes("repeated_references", "first_k", "next_k");
        Assertions.assertEquals("select `s`.`k` as `first_k`, `s`.`k` + 1 as `next_k`"
                        + " from `internal`.`view_binding`.`binding_source` `s`"
                        + " where `s`.`k` > 0 and `s`.`k` < 10",
                view.getInlineViewDef());
    }

    @Test
    public void testArithmeticImplicitOutputName() throws Exception {
        createView("create view arithmetic_implicit_name as select k + 1 from binding_source");

        View view = assertDefinitionAnalyzes("arithmetic_implicit_name", "__add_0");
        Assertions.assertTrue(view.getInlineViewDef().contains("`binding_source`.`k` + 1"),
                view.getInlineViewDef());
    }

    @Test
    public void testInferredSchemaKeepsQueryAliasScope() throws Exception {
        createView("create view ordered_implicit_name as select k + 1 from binding_source"
                + " order by `k + 1` limit 2");
        View view = assertDefinitionAnalyzes("ordered_implicit_name", "__add_0");
        Assertions.assertTrue(view.getInlineViewDef().contains("order by `k + 1` limit 2"),
                view.getInlineViewDef());

        createView("create view duplicate_implicit_names as select k + 1, k + 1 from binding_source");
        assertDefinitionAnalyzes("duplicate_implicit_names", "__add_0", "__add_1");
    }

    @Test
    public void testVariantImplicitOutputNames() throws Exception {
        createView("create view dotted_implicit_name as select payload.video_id from binding_source");
        View dottedView = assertDefinitionAnalyzes("dotted_implicit_name", "payload.video_id");
        Assertions.assertTrue(dottedView.getInlineViewDef().contains("`payload`.`video_id`"),
                dottedView.getInlineViewDef());

        createView("create view parenthesized_implicit_name as select (payload.video_id) from binding_source");
        View parenthesizedView = assertDefinitionAnalyzes("parenthesized_implicit_name", "payload.video_id");
        Assertions.assertTrue(parenthesizedView.getInlineViewDef().contains(
                "`payload`.`video_id`) AS `payload.video_id`"), parenthesizedView.getInlineViewDef());

        createView("create view bracket_implicit_name as select payload['video_id'] from binding_source");
        View bracketView = assertDefinitionAnalyzes("bracket_implicit_name", "__element_at_0");
        Assertions.assertTrue(bracketView.getInlineViewDef().contains("`payload`['video_id']"),
                bracketView.getInlineViewDef());
    }

    @Test
    public void testDerivedRelationKeepsImplicitOutputNames() throws Exception {
        createView("create view subquery_implicit_name as select q.`k + 1`"
                + " from (select k + 1 from binding_source) q");
        assertDefinitionAnalyzes("subquery_implicit_name", "__add_0");

        createView("create view cte_implicit_name as with q as (select k + 1 from binding_source)"
                + " select q.`k + 1` from q");
        assertDefinitionAnalyzes("cte_implicit_name", "__add_0");
    }

    @Test
    public void testLambdaImplicitOutputNameWithoutTable() throws Exception {
        createView("create view lambda_implicit_name as select q.`array_map(x -> x + 1, [1,2])`"
                + " from (select array_map(x -> x + 1, [1,2])) q");

        assertDefinitionAnalyzes("lambda_implicit_name", "__array_map_0");
    }

    @Test
    public void testAliasStarDoesNotRewriteAggregateInputs() throws Exception {
        createView("create view aggregate_alias_star as with source_rows as (select k, v from binding_source)"
                + " select q.*, q.total + 1 as next_total"
                + " from (select k, sum(v) as total from source_rows group by k) q");

        View view = assertDefinitionAnalyzes("aggregate_alias_star", "k", "total", "next_total");
        Assertions.assertTrue(view.getInlineViewDef().contains("sum(`source_rows`.`v`)"),
                view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("group by `source_rows`.`k`"),
                view.getInlineViewDef());
        Assertions.assertFalse(view.getInlineViewDef().contains("sum(`q`."), view.getInlineViewDef());
    }

    @Test
    public void testVariantPathsSurviveCreateAndAlter() throws Exception {
        createView("create view variant_paths as select cast(s.payload.video_id as bigint) as dotted_id,"
                + " cast(s.payload['video_id'] as bigint) as bracket_id from binding_source s");

        View view = assertDefinitionAnalyzes("variant_paths", "dotted_id", "bracket_id");
        Assertions.assertTrue(view.getInlineViewDef().contains("`s`.`payload`.`video_id`"),
                view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("`s`.`payload`['video_id']"),
                view.getInlineViewDef());

        alterView("alter view variant_paths as select cast(payload.session.duration as bigint) as duration,"
                + " cast(payload['session']['duration'] as bigint) as bracket_duration from binding_source");
        view = assertDefinitionAnalyzes("variant_paths", "duration", "bracket_duration");
        Assertions.assertTrue(view.getInlineViewDef().contains("`payload`.`session`.`duration`"),
                view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("`payload`['session']['duration']"),
                view.getInlineViewDef());
    }

    @Test
    public void testCorrelatedReferencesKeepTheirScope() throws Exception {
        createView("create view correlated_references as select outer_row.k from binding_source outer_row"
                + " where exists (select 1 from binding_source inner_row"
                + " where inner_row.k = outer_row.k and inner_row.v > outer_row.v)");

        View view = assertDefinitionAnalyzes("correlated_references", "k");
        Assertions.assertTrue(view.getInlineViewDef().contains("`inner_row`.`k` = `outer_row`.`k`"),
                view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("`inner_row`.`v` > `outer_row`.`v`"),
                view.getInlineViewDef());
    }

    @Test
    public void testExceptAndAdditionalProjection() throws Exception {
        createView("create view except_projection as select * except (payload, items), k + v as total"
                + " from binding_source");

        View view = assertDefinitionAnalyzes("except_projection", "k", "v", "total");
        Assertions.assertFalse(view.getInlineViewDef().contains("except"), view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("`binding_source`.`k`"),
                view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("`binding_source`.`v`"),
                view.getInlineViewDef());
    }

    @Test
    public void testExceptInAggregate() throws Exception {
        createView("create view except_aggregate as select * except (v, payload, items), sum(v) as total"
                + " from binding_source group by k");

        View view = assertDefinitionAnalyzes("except_aggregate", "k", "total");
        Assertions.assertFalse(view.getInlineViewDef().contains("except"), view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("sum(`internal`.`view_binding`.`binding_source`.`v`)"),
                view.getInlineViewDef());
    }

    @Test
    public void testReplacePreservesTheExpression() throws Exception {
        createView("create view replace_expression as select * replace ((v + 1) * 2 as v)"
                + " except (payload, items), k + 3 as next_k from binding_source");

        View view = assertDefinitionAnalyzes("replace_expression", "k", "v", "next_k");
        Assertions.assertTrue(view.getInlineViewDef().contains("+ 1"), view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("* 2"), view.getInlineViewDef());
        Assertions.assertFalse(view.getInlineViewDef().contains("replace"), view.getInlineViewDef());
        Assertions.assertFalse(view.getInlineViewDef().contains("except"), view.getInlineViewDef());
    }

    @Test
    public void testSelectAliasesRemainValidInGroupHavingAndOrder() throws Exception {
        createView("create view select_aliases as select k as grouping_key, sum(v) as total"
                + " from binding_source group by grouping_key having total > 0 order by total");

        View view = assertDefinitionAnalyzes("select_aliases", "grouping_key", "total");
        Assertions.assertTrue(view.getInlineViewDef().contains("sum(`internal`.`view_binding`.`binding_source`.`v`)"),
                view.getInlineViewDef());
        Assertions.assertFalse(view.getInlineViewDef().contains("`binding_source`.`total`"),
                view.getInlineViewDef());
        Assertions.assertFalse(view.getInlineViewDef().contains("`binding_source`.`grouping_key`"),
                view.getInlineViewDef());
    }

    @Test
    public void testGroupByExpressionAliasKeepsTheSourceReference() throws Exception {
        createView("create view group_expression_alias as select k + 1 as k1, sum(v) as total"
                + " from binding_source group by k1");

        View view = assertDefinitionAnalyzes("group_expression_alias", "k1", "total");
        Assertions.assertTrue(view.getInlineViewDef().contains("`binding_source`.`k` + 1 as `k1`"),
                view.getInlineViewDef());
        Assertions.assertFalse(view.getInlineViewDef().contains("`binding_source`.`k1`"),
                view.getInlineViewDef());
    }

    @Test
    public void testRelationAliasTakesPriorityOverOutputAlias() throws Exception {
        createView("create view relation_and_output_alias as select x.v as x"
                + " from binding_source x order by x.v");

        View view = assertDefinitionAnalyzes("relation_and_output_alias", "x");
        Assertions.assertEquals("select `x`.`v` as `x` from `internal`.`view_binding`.`binding_source` `x`"
                + " order by `x`.`v`", view.getInlineViewDef());
    }

    @Test
    public void testDereferenceOfExpressionPreservesInnerColumn() throws Exception {
        createView("create view expression_dereference as select named_struct('value', k).value as field_value"
                + " from binding_source");

        View view = assertDefinitionAnalyzes("expression_dereference", "field_value");
        Assertions.assertTrue(view.getInlineViewDef().contains(
                "named_struct('value', `internal`.`view_binding`.`binding_source`.`k`).value"),
                view.getInlineViewDef());
    }

    @Test
    public void testExplicitColumnsAfterStarExpansion() throws Exception {
        createView("create view explicit_columns(first_value, second_value) as"
                + " select * except (payload, items) from binding_source");

        View view = assertDefinitionAnalyzes("explicit_columns", "first_value", "second_value");
        Assertions.assertTrue(view.getInlineViewDef().contains("AS `first_value`"), view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("AS `second_value`"), view.getInlineViewDef());
    }

    @Test
    public void testLambdaFieldAccessAndOuterReference() throws Exception {
        createView("create view lambda_fields as select array_map(x -> x.value + k, x.items) as mapped"
                + " from binding_source x");

        View view = assertDefinitionAnalyzes("lambda_fields", "mapped");
        Assertions.assertTrue(view.getInlineViewDef().contains("`x`.`value`"), view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("`x`.`k`"), view.getInlineViewDef());
        Assertions.assertTrue(view.getInlineViewDef().contains("`x`.`items`"), view.getInlineViewDef());
    }

    @Test
    public void testNestedDefinitionsAndSubsequentStatementsAreIndependent() throws Exception {
        createView("create view inner_definition as select k, cast(payload.video_id as bigint) as video_id"
                + " from binding_source");
        View innerView = assertDefinitionAnalyzes("inner_definition", "k", "video_id");
        String innerDefinition = innerView.getInlineViewDef();

        createView("create view outer_definition as select v.*, v.video_id + 1 as next_id"
                + " from inner_definition v where v.k > 0");
        assertDefinitionAnalyzes("outer_definition", "k", "video_id", "next_id");
        PlanChecker.from(connectContext).analyze("select s.k from binding_source s where s.v > 0");

        createView("create view independent_definition as select v from binding_source");
        View independentView = assertDefinitionAnalyzes("independent_definition", "v");
        Assertions.assertEquals("select `internal`.`view_binding`.`binding_source`.`v`"
                + " from `internal`.`view_binding`.`binding_source`", independentView.getInlineViewDef());
        Assertions.assertEquals(innerDefinition, innerView.getInlineViewDef());
    }

    @Test
    public void testFailedCreateDoesNotLeakRewriteState() throws Exception {
        String failedSql = "create view definition_after_failure as select k, missing_column from binding_source";
        CreateViewCommand command = (CreateViewCommand) new NereidsParser().parseSingle(failedSql);
        StmtExecutor executor = new StmtExecutor(connectContext, failedSql);
        ViewSqlRewriter previousRewriter = connectContext.getStatementContext().getViewSqlRewriter();
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> command.run(connectContext, executor));
        Assertions.assertTrue(exception.getMessage().contains("missing_column"), exception.getMessage());
        Assertions.assertSame(previousRewriter, connectContext.getStatementContext().getViewSqlRewriter());

        createView("create view definition_after_failure as select k from binding_source");
        View view = assertDefinitionAnalyzes("definition_after_failure", "k");
        Assertions.assertEquals("select `internal`.`view_binding`.`binding_source`.`k`"
                + " from `internal`.`view_binding`.`binding_source`", view.getInlineViewDef());

        alterView("alter view definition_after_failure as select v from binding_source");
        view = assertDefinitionAnalyzes("definition_after_failure", "v");
        Assertions.assertEquals("select `internal`.`view_binding`.`binding_source`.`v`"
                + " from `internal`.`view_binding`.`binding_source`", view.getInlineViewDef());
    }

    @Test
    public void testOrdinaryAmbiguousReferenceStillFails() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze("select k from binding_source a"
                        + " join binding_source b on a.k = b.k"));
        Assertions.assertTrue(exception.getMessage().contains("k is ambiguous"), exception.getMessage());
    }

    private View assertDefinitionAnalyzes(String viewName, String... columnNames) throws Exception {
        View view = (View) Env.getCurrentInternalCatalog().getDbOrDdlException("view_binding")
                .getTableOrDdlException(viewName);
        Assertions.assertEquals(Arrays.asList(columnNames), view.getFullSchema().stream()
                .map(Column::getName).collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList(columnNames), PlanChecker.from(connectContext)
                .analyze(view.getInlineViewDef()).getPlan().getOutput().stream()
                .map(NamedExpression::getName).collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList(columnNames), PlanChecker.from(connectContext)
                .analyze("select * from view_binding." + viewName).getPlan().getOutput().stream()
                .map(NamedExpression::getName).collect(Collectors.toList()));
        return view;
    }

    private void alterView(String sql) throws Exception {
        AlterViewCommand command = (AlterViewCommand) new NereidsParser().parseSingle(sql);
        command.run(connectContext, new StmtExecutor(connectContext, sql));
    }
}
