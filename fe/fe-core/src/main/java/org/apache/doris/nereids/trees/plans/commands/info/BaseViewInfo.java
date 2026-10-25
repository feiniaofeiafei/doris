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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionSeqContext;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.AnalyzeCTE;
import org.apache.doris.nereids.rules.analysis.BindExpression;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.analysis.CheckPolicy;
import org.apache.doris.nereids.rules.analysis.EliminateLogicalPreAggOnHint;
import org.apache.doris.nereids.rules.analysis.EliminateLogicalSelectHint;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.types.ConnectorComputeVariantType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.tso.MasterTsoProvider;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** BaseViewInfo */
public class BaseViewInfo {
    protected final TableNameInfo viewName;
    protected LogicalPlan logicalQuery;
    protected final String querySql;
    protected final List<SimpleColumnDefinition> simpleColumnDefinitions;
    protected final List<Column> finalCols = Lists.newArrayList();
    protected Plan analyzedPlan;

    public BaseViewInfo(TableNameInfo viewName,
            String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        this.viewName = viewName;
        this.querySql = querySql;
        this.simpleColumnDefinitions = simpleColumnDefinitions;
    }

    protected void analyzeViewDefinition(String sql, ConnectContext ctx) throws AnalysisException {
        StatementContext stmtCtx = ctx.getStatementContext();
        LogicalPlan parsedViewPlan = new NereidsParser().parseForCreateView(sql, stmtCtx.getViewSqlRewriter());
        logicalQuery = parsedViewPlan;
        if (logicalQuery instanceof LogicalFileSink) {
            throw new AnalysisException("Not support OUTFILE clause in CREATE VIEW statement");
        }
        CascadesContext viewContext = CascadesContext.initContext(
                stmtCtx, parsedViewPlan, PhysicalProperties.ANY);
        AnalyzerForCreateView analyzer = new AnalyzerForCreateView(viewContext);
        analyzer.analyze();
        analyzedPlan = viewContext.getRewritePlan();
    }

    /**
     * Add the full path to the field
     *
     * @param indexStringSqlMap key is the start and end position of the sql substring that needs to be replaced,
     *         and value is the new string used for replacement.
     * @param querySql origin query sql
     * @return sql rewritten sql
     */
    public static String rewriteSql(TreeMap<Pair<Integer, Integer>, String> indexStringSqlMap, String querySql) {
        StringBuilder builder = new StringBuilder();
        int beg = 0;
        for (Map.Entry<Pair<Integer, Integer>, String> entry : indexStringSqlMap.entrySet()) {
            Pair<Integer, Integer> index = entry.getKey();
            builder.append(querySql, beg, index.first);
            builder.append(entry.getValue());
            beg = index.second + 1;
        }
        builder.append(querySql, beg, querySql.length());
        return builder.toString();
    }

    protected String rewriteProjectsToUserDefineAlias(String resSql) {
        List<String> finalColNames = finalCols.stream().map(Column::getName).collect(Collectors.toList());
        if (simpleColumnDefinitions.isEmpty()) {
            PlanUtils.OutermostPlanFinderContext finder = new PlanUtils.OutermostPlanFinderContext();
            analyzedPlan.accept(PlanUtils.OutermostPlanFinder.INSTANCE, finder);
            LogicalResultSink<?> sink = (LogicalResultSink<?>) finder.outermostPlan;
            List<String> queryNames = sink.child().getOutput().stream()
                    .map(Slot::getName).collect(Collectors.toList());
            if (queryNames.equals(finalColNames)) {
                return resSql;
            }
            // ResultSink can infer external names such as __add_0. Preserve that existing schema
            // contract by ordinal without changing aliases referenced by ORDER BY/GROUP BY inside
            // the query. A CTE column list also handles duplicate internal expression names.
            String columns = finalColNames.stream()
                    .map(name -> Utils.qualifiedNameWithBackquote(ImmutableList.of(name)))
                    .collect(Collectors.joining(", "));
            return "WITH `__doris_view_definition` (" + columns + ") AS (" + resSql
                    + ") SELECT * FROM `__doris_view_definition`";
        }
        return rewriteProjectsToUserDefineAlias(resSql, finalColNames);
    }

    /**
     * rewrite projects to user define alias by column names list
     */
    public static String rewriteProjectsToUserDefineAlias(String resSql, List<String> finalColNames) {
        if (finalColNames.isEmpty()) {
            return resSql;
        }
        IndexFinder finder = new IndexFinder();
        ParserRuleContext tree = NereidsParser.toAst(resSql, DorisParser::singleStatement);
        finder.visit(tree);
        List<NamedExpressionContext> namedExpressionContexts = finder.getNamedExpressionContexts();
        StringBuilder replaceWithColsBuilder = new StringBuilder();
        for (int i = 0; i < namedExpressionContexts.size(); ++i) {
            NamedExpressionContext namedExpressionContext = namedExpressionContexts.get(i);
            int start = namedExpressionContext.expression().start.getStartIndex();
            int stop = namedExpressionContext.expression().stop.getStopIndex();
            replaceWithColsBuilder.append(resSql, start, stop + 1);
            replaceWithColsBuilder.append(" AS `");
            String escapeBacktick = finalColNames.get(i).replace("`", "``");
            replaceWithColsBuilder.append(escapeBacktick);
            replaceWithColsBuilder.append('`');
            if (i != namedExpressionContexts.size() - 1) {
                replaceWithColsBuilder.append(", ");
            }
        }
        String replaceWithCols = replaceWithColsBuilder.toString();
        return StringUtils.overlay(resSql, replaceWithCols, finder.getIndex().first,
                finder.getIndex().second + 1);
    }

    protected void createFinalCols(List<Slot> outputs) throws org.apache.doris.common.AnalysisException {
        if (simpleColumnDefinitions.isEmpty()) {
            for (Slot output : outputs) {
                DataType dataType = TypeCoercionUtils.replaceSpecifiedType(output.getDataType(), NullType.class,
                        TinyIntType.INSTANCE);
                // View schemas are persisted, so strip connector execution markers without changing
                // unrelated legacy types at this durable boundary.
                dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                        ConnectorComputeVariantType.class, VariantType.INSTANCE);
                Column column = new Column(output.getName(), dataType.toCatalogDataType(), output.nullable());
                finalCols.add(column);
            }
        } else {
            if (outputs.size() != simpleColumnDefinitions.size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            for (int i = 0; i < simpleColumnDefinitions.size(); ++i) {
                Slot output = outputs.get(i);
                DataType dataType = TypeCoercionUtils.replaceSpecifiedType(output.getDataType(), NullType.class,
                        TinyIntType.INSTANCE);
                // Keep explicit-column views on the same durable type boundary as inferred-column views.
                dataType = TypeCoercionUtils.replaceSpecifiedType(dataType,
                        ConnectorComputeVariantType.class, VariantType.INSTANCE);
                Column column = new Column(simpleColumnDefinitions.get(i).getName(),
                        dataType.toCatalogDataType(), output.nullable());
                column.setComment(simpleColumnDefinitions.get(i).getComment());
                finalCols.add(column);
            }
        }
    }

    /**validate*/
    public void validate(ConnectContext ctx) throws UserException {
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.planWithLock(new UnboundResultSink<>(logicalQuery), PhysicalProperties.ANY, ExplainLevel.NONE);
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
            try {
                FeNameFormat.checkColumnName(col.getName());
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e);
            }
        }
    }

    /** traverse ast to find the outermost project list location information in sql*/
    protected static class IndexFinder extends DorisParserBaseVisitor<Void> {
        private boolean found = false;
        private int startIndex;
        private int stopIndex;
        private List<NamedExpressionContext> namedExpressionContexts = Lists.newArrayList();

        @Override
        public Void visitChildren(RuleNode node) {
            if (found) {
                return null;
            }
            int n = node.getChildCount();
            for (int i = 0; i < n; ++i) {
                ParseTree c = node.getChild(i);
                c.accept(this);
            }
            return null;
        }

        @Override
        public Void visitCte(DorisParser.CteContext ctx) {
            return null;
        }

        @Override
        public Void visitSelectColumnClause(DorisParser.SelectColumnClauseContext ctx) {
            if (found) {
                return null;
            }
            startIndex = ctx.getStart().getStartIndex();
            stopIndex = ctx.getStop().getStopIndex();
            found = true;

            NamedExpressionSeqContext namedExpressionSeqContext = ctx.namedExpressionSeq();
            namedExpressionContexts = namedExpressionSeqContext.namedExpression();
            return null;
        }

        public Pair<Integer, Integer> getIndex() {
            return Pair.of(startIndex, stopIndex);
        }

        public List<NamedExpressionContext> getNamedExpressionContexts() {
            return namedExpressionContexts;
        }
    }

    /**AnalyzerForCreateView*/
    protected static class AnalyzerForCreateView extends AbstractBatchJobExecutor {
        private final List<RewriteJob> jobs;

        public AnalyzerForCreateView(CascadesContext cascadesContext) {
            super(cascadesContext);
            jobs = buildAnalyzeJobs();
        }

        public void analyze() {
            // View SQL is bound before the normal planner runs. Collect nested scans and
            // obtain the statement's TTL reference before BindRelation needs it.
            cascadesContext.newTableCollector(true).collect();
            StatementContext statementContext = cascadesContext.getStatementContext();
            if (statementContext.isChangelogReferenceTsoRequired()) {
                statementContext.getOrRegisterChangelogReferenceTso(
                        () -> MasterTsoProvider.getCurrentTso(statementContext.getConnectContext()));
            }
            cascadesContext.setCteContext(new CTEContext());
            execute();
        }

        @Override
        public List<RewriteJob> getJobs() {
            return jobs;
        }

        private static List<RewriteJob> buildAnalyzeJobs() {
            return notTraverseChildrenOf(
                ImmutableSet.of(LogicalView.class, LogicalCTEAnchor.class),
                AnalyzerForCreateView::buildAnalyzerJobs
            );
        }

        private static List<RewriteJob> buildAnalyzerJobs() {
            return jobs(
                topDown(new EliminateLogicalSelectHint(),
                    new EliminateLogicalPreAggOnHint()),
                topDown(new AnalyzeCTE()),
                bottomUp(
                    new BindRelation(),
                    new CheckPolicy(),
                    new BindExpression()
                )
            );
        }
    }

    protected void checkViewSql(String viewSql) {
        // Validate the definition that will actually be persisted. A normal parse also prevents
        // the validation pass from recording source edits into the enclosing statement context.
        LogicalPlan parsed = new NereidsParser().parseSingle(viewSql);
        logicalQuery = (LogicalPlan) ((UnboundResultSink<?>) parsed).child();
    }
}
