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

package org.apache.doris.nereids.util;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ViewSqlRewriterTest {
    @Test
    void acceptsRepeatedIdenticalBindings() {
        String sql = "SELECT a FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> column = span(sql, "a");
        rewriter.bindColumn(column, ImmutableList.of("t", "a"));
        rewriter.bindColumn(column, ImmutableList.of("t", "a"));

        Assertions.assertEquals("SELECT `t`.`a` FROM t", rewriter.rewrite(sql));
        Assertions.assertEquals("SELECT `t`.`a` FROM t", rewriter.rewrite(sql));
    }

    @Test
    void rejectsConflictingBindingsOfOneOccurrence() {
        String sql = "SELECT a FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> column = span(sql, "a");
        rewriter.bindColumn(column, ImmutableList.of("t", "a"));

        Assertions.assertThrows(IllegalStateException.class,
                () -> rewriter.bindColumn(column, ImmutableList.of("other", "a")));
    }

    @Test
    void rejectsOverlappingReferencesWithoutAStarOwner() {
        String sql = "SELECT payload.field FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        rewriter.bindColumn(span(sql, "payload.field"), ImmutableList.of("t", "payload", "field"));
        rewriter.bindColumn(span(sql, "field"), ImmutableList.of("field"));

        Assertions.assertThrows(IllegalStateException.class, () -> rewriter.rewrite(sql));
    }

    @Test
    void starConsumesExceptReferencesAndPreservesOtherProjections() {
        String sql = "SELECT * EXCEPT (hidden), value + 1 AS next FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        rewriter.bindColumn(span(sql, "hidden"), ImmutableList.of("t", "hidden"));
        rewriter.bindColumn(span(sql, "value"), ImmutableList.of("t", "value"));
        rewriter.expandStar(span(sql, "* EXCEPT (hidden)"), ImmutableList.of(slot("t", "kept")));

        Assertions.assertEquals("SELECT `t`.`kept`, `t`.`value` + 1 AS next FROM t", rewriter.rewrite(sql));
    }

    @Test
    void replacementPreservesSourceSyntaxAndUsesBoundOutputOrder() {
        String sql = "SELECT * REPLACE (a /* keep this comment */ + 1 AS a) FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> star = span(sql, "* REPLACE (a /* keep this comment */ + 1 AS a)");
        Pair<Integer, Integer> expression = span(sql, "a /* keep this comment */ + 1");
        rewriter.registerStarExpression(star, "A", expression);
        rewriter.bindColumn(Pair.of(expression.first, expression.first), ImmutableList.of("t", "a"));
        rewriter.expandStar(star, ImmutableList.of(slot("t", "b"),
                new Alias(new Add(slot("t", "a"), new IntegerLiteral(1)), "a")));

        Assertions.assertEquals("SELECT `t`.`b`, `t`.`a` /* keep this comment */ + 1 AS `a` FROM t",
                rewriter.rewrite(sql));
    }

    @Test
    void rendersInnerStarBeforeItsContainingReplacement() {
        String sql = "SELECT * REPLACE ((SELECT * EXCEPT (drop_me) FROM s) AS a) FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> outerStar = span(sql, "* REPLACE ((SELECT * EXCEPT (drop_me) FROM s) AS a)");
        Pair<Integer, Integer> expression = span(sql, "(SELECT * EXCEPT (drop_me) FROM s)");
        rewriter.registerStarExpression(outerStar, "a", expression);
        rewriter.expandStar(outerStar, ImmutableList.of(new Alias(new IntegerLiteral(1), "a"), slot("t", "b")));
        rewriter.expandStar(span(sql, "* EXCEPT (drop_me)"), ImmutableList.of(slot("s", "value")));
        rewriter.bindColumn(span(sql, "drop_me"), ImmutableList.of("s", "drop_me"));
        int relation = sql.indexOf("FROM s") + "FROM ".length();
        rewriter.replace(Pair.of(relation, relation), "`db`.`s`");

        String expected = "SELECT (SELECT `s`.`value` FROM `db`.`s`) AS `a`, `t`.`b` FROM t";
        Assertions.assertEquals(expected, rewriter.rewrite(sql));
        Assertions.assertEquals(expected, rewriter.rewrite(sql));
    }

    @Test
    void rejectsAnEditCrossingTheEndOfAStar() {
        String sql = "SELECT * EXCEPT (hidden) FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> star = span(sql, "* EXCEPT (hidden)");
        rewriter.expandStar(star, ImmutableList.of(slot("t", "kept")));
        rewriter.replace(Pair.of(star.second, star.second + 2), "invalid");

        Assertions.assertThrows(IllegalStateException.class, () -> rewriter.rewrite(sql));
    }

    @Test
    void rejectsAnEditCrossingTheStartOfAStar() {
        String sql = "SELECT * EXCEPT (hidden) FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> star = span(sql, "* EXCEPT (hidden)");
        rewriter.expandStar(star, ImmutableList.of(slot("t", "kept")));
        rewriter.replace(Pair.of(star.first - 1, star.first), "invalid");

        Assertions.assertThrows(IllegalStateException.class, () -> rewriter.rewrite(sql));
    }

    @Test
    void preservesImplicitOutputNameAfterBindingChildReferences() {
        String sql = "SELECT a /* keep this comment */ + 1 FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> expression = span(sql, "a /* keep this comment */ + 1");
        rewriter.preserveOutputName(expression, "(a + 1)");
        rewriter.bindColumn(Pair.of(expression.first, expression.first), ImmutableList.of("t", "a"));

        Assertions.assertEquals("SELECT `t`.`a` /* keep this comment */ + 1 AS `(a + 1)` FROM t",
                rewriter.rewrite(sql));
    }

    @Test
    void escapesBackticksInPreservedOutputNames() {
        String sql = "SELECT `odd``name` FROM t";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> column = span(sql, "`odd``name`");
        rewriter.preserveOutputName(column, "odd`name");
        rewriter.bindColumn(column, ImmutableList.of("t", "odd`name"));

        Assertions.assertEquals("SELECT `t`.`odd``name` AS `odd``name` FROM t", rewriter.rewrite(sql));
    }

    @Test
    void leavesUnchangedExpressionsWithoutAnAlias() {
        String sql = "SELECT 1 + 2, 3 /* keep this comment */ + 4";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> firstExpression = span(sql, "1 + 2");
        rewriter.preserveOutputName(firstExpression, "(1 + 2)");
        rewriter.replace(firstExpression, "1 + 2");
        rewriter.preserveOutputName(span(sql, "3 /* keep this comment */ + 4"), "(3 + 4)");

        Assertions.assertEquals(sql, rewriter.rewrite(sql));
    }

    @Test
    void rendersInnerStarBeforePreservingSubqueryOutputName() {
        String sql = "SELECT (SELECT * EXCEPT (hidden) FROM s)";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        String expression = "(SELECT * EXCEPT (hidden) FROM s)";
        rewriter.preserveOutputName(span(sql, expression), expression);
        rewriter.expandStar(span(sql, "* EXCEPT (hidden)"), ImmutableList.of(slot("s", "value")));
        rewriter.bindColumn(span(sql, "hidden"), ImmutableList.of("s", "hidden"));

        String expected = "SELECT (SELECT `s`.`value` FROM s) AS `(SELECT * EXCEPT (hidden) FROM s)`";
        Assertions.assertEquals(expected, rewriter.rewrite(sql));
        Assertions.assertEquals(expected, rewriter.rewrite(sql));
    }

    @Test
    void preservesColumnNamesOnlyForRegisteredOutputOccurrences() {
        String sql = "SELECT value FROM t WHERE value > 0";
        ViewSqlRewriter rewriter = new ViewSqlRewriter();
        Pair<Integer, Integer> projection = span(sql, "value");
        int predicateStart = sql.lastIndexOf("value");
        Pair<Integer, Integer> predicate = Pair.of(predicateStart, predicateStart + "value".length() - 1);
        rewriter.registerColumnOutput(projection, projection);
        rewriter.bindColumn(projection, ImmutableList.of("t", "value"));
        rewriter.bindColumnOutput(projection, "value");
        rewriter.bindColumn(predicate, ImmutableList.of("t", "value"));
        rewriter.bindColumnOutput(predicate, "value");

        Assertions.assertEquals("SELECT `t`.`value` AS `value` FROM t WHERE `t`.`value` > 0",
                rewriter.rewrite(sql));
    }

    private static SlotReference slot(String qualifier, String name) {
        return new SlotReference(name, IntegerType.INSTANCE, true, ImmutableList.of(qualifier));
    }

    private static Pair<Integer, Integer> span(String sql, String fragment) {
        int start = sql.indexOf(fragment);
        Assertions.assertTrue(start >= 0, "Missing source fragment: " + fragment);
        return Pair.of(start, start + fragment.length() - 1);
    }
}
