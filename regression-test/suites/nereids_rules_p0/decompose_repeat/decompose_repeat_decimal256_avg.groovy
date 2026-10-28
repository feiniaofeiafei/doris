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

// Reproducer for PR #68936: the rewritten query must preserve the AVG result type.
suite("decompose_repeat_decimal256_avg") {
    sql "drop table if exists decompose_repeat_decimal256_avg"
    sql """create table decompose_repeat_decimal256_avg (
        a int, b int, c int, v decimal(18, 2)
    ) distributed by hash(a) buckets 3 properties('replication_num'='1')"""
    sql """insert into decompose_repeat_decimal256_avg values
        (1,1,1,10),(1,1,1,20),(1,1,2,100),
        (1,2,1,null),(null,1,1,40),(2,2,2,null)"""

    def query = """select a,b,c,avg(v)
        from decompose_repeat_decimal256_avg group by rollup(a,b,c)
        order by a,b,c,avg(v)"""
    for (decimal256 in [false, true]) {
        sql "set enable_decimal256=${decimal256}"
        sql "set disable_nereids_rules='DECOMPOSE_REPEAT'"
        def baseline = sql(query)
        logger.info("decimal256=${decimal256}, rewrite disabled: ${baseline}")
        sql "set disable_nereids_rules=''"
        explain {
            sql query
            contains "avg_combine"
            contains "avg_merge"
            contains "avg_finalize"
        }
        logger.info("decimal256=${decimal256}, executing rewritten query")
        def rewritten = sql(query)
        assertEquals(baseline, rewritten)
    }
}
