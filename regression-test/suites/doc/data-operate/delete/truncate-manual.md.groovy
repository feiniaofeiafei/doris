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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/delete/truncate-manual.md") {
    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS example_db;
            USE example_db;
            CREATE TABLE IF NOT EXISTS tbl(
                a INT
            )
            PARTITION BY RANGE(a) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2)
            )
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql "TRUNCATE TABLE example_db.tbl;"
        sql "TRUNCATE TABLE tbl PARTITION(p1, p2);"
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/delete/truncate-manual.md failed to exec, please fix it", t)
    }
}
