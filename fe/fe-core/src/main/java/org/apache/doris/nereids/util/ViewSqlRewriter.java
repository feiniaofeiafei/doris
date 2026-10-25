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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Source edits for one view definition. Positions belong to parsed references, never to bound
 * slots. Most syntax is copied verbatim; only resolved names and star projections change.
 * A star owns its EXCEPT/REPLACE syntax, including edits inside replacement expressions.
 * Implicit projection names are preserved when edits change the expression's source text.
 */
public class ViewSqlRewriter {
    private final TreeMap<Pair<Integer, Integer>, String> replacements =
            new TreeMap<>(Comparator.<Pair<Integer, Integer>, Integer>comparing(span -> span.first)
                    .thenComparing(span -> span.second));
    private final Map<Pair<Integer, Integer>, Map<String, Pair<Integer, Integer>>> starExpressions = new HashMap<>();
    private final Map<Pair<Integer, Integer>, List<Projection>> starExpansions = new HashMap<>();
    private final Map<Pair<Integer, Integer>, Pair<Integer, Integer>> columnOutputs = new HashMap<>();
    private final Map<Pair<Integer, Integer>, String> outputNames = new HashMap<>();

    public void replace(Pair<Integer, Integer> source, String sql) {
        String previous = replacements.putIfAbsent(source, sql);
        Preconditions.checkState(previous == null || previous.equals(sql),
                "Conflicting view SQL replacements at %s: %s and %s", source, previous, sql);
    }

    public void bindColumn(Pair<Integer, Integer> source, List<String> qualifiedName) {
        replace(source, Utils.qualifiedNameWithBackquote(qualifiedName));
    }

    public void registerColumnOutput(Pair<Integer, Integer> source, Pair<Integer, Integer> projection) {
        columnOutputs.put(source, projection);
    }

    public void bindColumnOutput(Pair<Integer, Integer> source, String name) {
        if (columnOutputs.containsKey(source)) {
            preserveOutputName(columnOutputs.get(source), name);
        }
    }

    public void preserveOutputName(Pair<Integer, Integer> source, String name) {
        outputNames.put(source, name);
    }

    public void registerStarExpression(Pair<Integer, Integer> star, String name, Pair<Integer, Integer> expression) {
        starExpressions.computeIfAbsent(star, key -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                .put(name, expression);
    }

    /** Record actual ordered outputs, including REPLACE expressions, after star binding. */
    public void expandStar(Pair<Integer, Integer> source, List<? extends NamedExpression> outputs) {
        List<Projection> projections = new ArrayList<>(outputs.size());
        for (NamedExpression output : outputs) {
            if (output instanceof Alias) {
                Pair<Integer, Integer> expression = starExpressions.get(source).get(output.getName());
                Preconditions.checkNotNull(expression, "Missing source for star replacement %s", output.getName());
                projections.add(new Projection(expression,
                        " AS " + Utils.qualifiedNameWithBackquote(ImmutableList.of(output.getName()))));
            } else {
                Preconditions.checkState(output instanceof Slot, "Unexpected star output: %s", output);
                Slot slot = (Slot) output;
                projections.add(new Projection(null,
                        Utils.qualifiedNameWithBackquote(slot.getQualifier(), slot.getName())));
            }
        }
        starExpansions.put(source, projections);
    }

    /** Render the definition without mutating the collected bindings. */
    public String rewrite(String sql) {
        TreeMap<Pair<Integer, Integer>, String> edits = new TreeMap<>(replacements);
        // Render inner projections before a containing expression or star's REPLACE clause.
        List<Pair<Integer, Integer>> projections = new ArrayList<>(starExpansions.keySet());
        projections.addAll(outputNames.keySet());
        projections.sort(Comparator.comparingInt(span -> span.second - span.first));
        for (Pair<Integer, Integer> projection : projections) {
            String expanded;
            if (outputNames.containsKey(projection)) {
                expanded = render(sql, edits, projection.first, projection.second + 1);
                if (!expanded.equals(sql.substring(projection.first, projection.second + 1))) {
                    expanded += " AS " + Utils.qualifiedNameWithBackquote(
                            ImmutableList.of(outputNames.get(projection)));
                }
            } else {
                expanded = starExpansions.get(projection).stream().map(output ->
                        output.source == null ? output.suffix
                                : render(sql, edits, output.source.first, output.source.second + 1)
                                        + output.suffix).collect(Collectors.joining(", "));
            }
            NavigableMap<Pair<Integer, Integer>, String> children =
                    editsInRange(edits, projection.first, projection.second + 1);
            for (Pair<Integer, Integer> source : children.keySet()) {
                Preconditions.checkState(source.first >= projection.first && source.second <= projection.second,
                        "Crossing view SQL edits: %s and %s", projection, source);
            }
            children.clear();
            edits.put(projection, expanded);
        }
        return render(sql, edits, 0, sql.length());
    }

    private static String render(String sql, TreeMap<Pair<Integer, Integer>, String> edits, int start, int end) {
        StringBuilder result = new StringBuilder();
        int cursor = start;
        for (Map.Entry<Pair<Integer, Integer>, String> edit : editsInRange(edits, start, end).entrySet()) {
            Pair<Integer, Integer> source = edit.getKey();
            Preconditions.checkState(source.first >= cursor && source.second < end,
                    "Overlapping or out-of-range view SQL edit: %s in [%s, %s)", source, cursor, end);
            result.append(sql, cursor, source.first).append(edit.getValue());
            cursor = source.second + 1;
        }
        return result.append(sql, cursor, end).toString();
    }

    private static NavigableMap<Pair<Integer, Integer>, String> editsInRange(
            TreeMap<Pair<Integer, Integer>, String> edits, int start, int end) {
        Pair<Integer, Integer> first = Pair.of(start, Integer.MIN_VALUE);
        Map.Entry<Pair<Integer, Integer>, String> previous = edits.lowerEntry(first);
        if (previous != null && previous.getKey().second >= start) {
            // Include an edit crossing the left boundary so the caller rejects it.
            first = previous.getKey();
        }
        return edits.subMap(first, true, Pair.of(end, Integer.MIN_VALUE), false);
    }

    private static class Projection {
        private final Pair<Integer, Integer> source;
        private final String suffix;

        private Projection(Pair<Integer, Integer> source, String suffix) {
            this.source = source;
            this.suffix = suffix;
        }
    }
}
