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

package org.apache.doris.nereids.parser;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.AliasQueryContext;
import org.apache.doris.nereids.DorisParser.ColumnReferenceContext;
import org.apache.doris.nereids.DorisParser.DereferenceContext;
import org.apache.doris.nereids.DorisParser.IdentifierContext;
import org.apache.doris.nereids.DorisParser.LateralViewContext;
import org.apache.doris.nereids.DorisParser.MultipartIdentifierContext;
import org.apache.doris.nereids.DorisParser.NamedExpressionContext;
import org.apache.doris.nereids.DorisParser.PrimaryExpressionContext;
import org.apache.doris.nereids.DorisParser.ReplaceContext;
import org.apache.doris.nereids.DorisParser.StarContext;
import org.apache.doris.nereids.DorisParser.TableAliasContext;
import org.apache.doris.nereids.DorisParser.TableNameContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.nereids.util.ViewSqlRewriter;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.Map;

/**LogicalPlanBuilderForCreateView*/
public class LogicalPlanBuilderForCreateView extends LogicalPlanBuilder {
    private final ViewSqlRewriter rewriter;

    public LogicalPlanBuilderForCreateView(Map<Integer, ParserRuleContext> selectHintMap, ViewSqlRewriter rewriter) {
        super(selectHintMap);
        this.rewriter = rewriter;
    }

    @Override
    protected LogicalPlan withGenerate(LogicalPlan plan, LateralViewContext ctx) {
        rewriter.replace(
                Pair.of(ctx.tableName.start.getStartIndex(),
                        ctx.tableName.stop.getStopIndex()),
                Utils.qualifiedNameWithBackquote(ImmutableList.of(ctx.tableName.getText())));
        for (IdentifierContext colCtx : ctx.columnNames) {
            rewriter.replace(
                    Pair.of(colCtx.start.getStartIndex(),
                            colCtx.stop.getStopIndex()),
                    Utils.qualifiedNameWithBackquote(ImmutableList.of(colCtx.getText())));
        }
        return super.withGenerate(plan, ctx);
    }

    @Override
    public LogicalSubQueryAlias<Plan> visitAliasQuery(AliasQueryContext ctx) {
        rewriter.replace(
                Pair.of(ctx.identifier().start.getStartIndex(),
                        ctx.identifier().stop.getStopIndex()),
                Utils.qualifiedNameWithBackquote(ImmutableList.of(ctx.identifier().getText())));
        if (ctx.columnAliases() != null) {
            for (IdentifierContext colCtx : ctx.columnAliases().identifier()) {
                rewriter.replace(
                        Pair.of(colCtx.start.getStartIndex(),
                                colCtx.stop.getStopIndex()),
                        Utils.qualifiedNameWithBackquote(ImmutableList.of(colCtx.getText())));
            }
        }
        return super.visitAliasQuery(ctx);
    }

    @Override
    protected LogicalPlan withTableAlias(LogicalPlan plan, TableAliasContext ctx) {
        if (ctx.strictIdentifier() == null) {
            return plan;
        }
        String alias = ctx.strictIdentifier().getText();
        rewriter.replace(
                Pair.of(ctx.strictIdentifier().start.getStartIndex(),
                        ctx.strictIdentifier().stop.getStopIndex()),
                Utils.qualifiedNameWithBackquote(ImmutableList.of(alias)));
        return super.withTableAlias(plan, ctx);
    }

    @Override
    public LogicalPlan visitTableName(TableNameContext ctx) {
        LogicalPlan plan = super.visitTableName(ctx);
        MultipartIdentifierContext identifier = ctx.multipartIdentifier();
        return (LogicalPlan) plan.rewriteDownShortCircuit(node -> {
            if (node instanceof UnboundRelation) {
                UnboundRelation relation = (UnboundRelation) node;
                return relation.withIndexInSql(Pair.of(identifier.start.getStartIndex(),
                        identifier.stop.getStopIndex()));
            }
            return node;
        });
    }

    @Override
    public Expression visitStar(StarContext ctx) {
        Expression expr = super.visitStar(ctx);
        UnboundStar star = (UnboundStar) expr;
        Pair<Integer, Integer> source = Pair.of(ctx.start.getStartIndex(), ctx.stop.getStopIndex());
        for (DorisParser.ExceptOrReplaceContext clause : ctx.exceptOrReplace()) {
            if (clause instanceof ReplaceContext) {
                for (NamedExpressionContext replacement : ((ReplaceContext) clause).namedExpressionSeq()
                        .namedExpression()) {
                    rewriter.registerStarExpression(source,
                            visitIdentifierOrText(replacement.identifierOrText()),
                            Pair.of(replacement.expression().start.getStartIndex(),
                                    replacement.expression().stop.getStopIndex()));
                }
            }
        }
        return star.withIndexInSql(source);
    }

    @Override
    public NamedExpression visitNamedExpression(NamedExpressionContext ctx) {
        NamedExpression expression = super.visitNamedExpression(ctx);
        if (ctx.identifierOrText() != null) {
            String alias = visitIdentifierOrText(ctx.identifierOrText());
            rewriter.replace(
                    Pair.of(ctx.identifierOrText().start.getStartIndex(),
                            ctx.identifierOrText().stop.getStopIndex()),
                    Utils.qualifiedNameWithBackquote(ImmutableList.of(alias)));
        } else {
            Pair<Integer, Integer> source = Pair.of(ctx.expression().start.getStartIndex(),
                    ctx.expression().stop.getStopIndex());
            if (expression instanceof UnboundAlias) {
                rewriter.preserveOutputName(
                        source, ((UnboundAlias) expression).getAlias().get());
            } else if (expression instanceof UnboundSlot) {
                rewriter.registerColumnOutput(
                        ((UnboundSlot) expression).getIndexInSqlString().get(), source);
            }
        }
        return expression;
    }

    @Override
    public Expression visitFunctionCallExpression(DorisParser.FunctionCallExpressionContext ctx) {
        Expression expr = super.visitFunctionCallExpression(ctx);
        if (expr instanceof UnboundFunction) {
            UnboundFunction function = (UnboundFunction) expr;
            function = function.withIndexInSql(Pair.of(
                    ctx.functionIdentifier().start.getStartIndex(),
                    ctx.functionIdentifier().stop.getStopIndex()));
            return function;
        } else {
            return expr;
        }
    }

    @Override
    protected Expression buildDereference(
            Expression base, DereferenceContext ctx, PrimaryExpressionContext originContext) {
        Expression expression = super.buildDereference(base, ctx, originContext);
        if (expression instanceof UnboundSlot) {
            return ((UnboundSlot) expression).withIndexInSql(Pair.of(
                    originContext.start.getStartIndex(), ctx.stop.getStopIndex()));
        }
        // Dereferencing an expression (for example named_struct(...).field) keeps its source syntax.
        return expression;
    }

    @Override
    public Expression visitColumnReference(ColumnReferenceContext ctx) {
        Expression expr = super.visitColumnReference(ctx);
        UnboundSlot slot = (UnboundSlot) expr;
        return slot.withIndexInSql(Pair.of(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

}
