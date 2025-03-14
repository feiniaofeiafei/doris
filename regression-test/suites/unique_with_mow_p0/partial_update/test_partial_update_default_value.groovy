
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

suite("test_primary_key_partial_update_default_value", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")

        connect( context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            sql "use ${db};"

            def tableName = "test_primary_key_partial_update_default_value"

            // create table
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
                        `score` int(11) NOT NULL COMMENT "用户得分",
                        `test` int(11) NULL DEFAULT "4321" COMMENT  "test",
                        `dft` int(11) DEFAULT "4321")
                        UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                        "store_row_column" = "${use_row_store}"); """
            // insert 2 lines
            sql """
                insert into ${tableName} values(2, "doris2", 2000, 223, 1)
            """

            sql """
                insert into ${tableName} values(1, "doris", 1000, 123, 1)
            """

            // stream load with key not exit before
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'format', 'csv'
                set 'partial_columns', 'true'
                set 'columns', 'id,score'

                file 'default.csv'
                time 10000 // limit inflight 10s
            }

            sql "sync"

            qt_select_default """
                select * from ${tableName} order by id;
            """

            // test special default values
            tableName = "test_primary_key_partial_update_default_value2"
            // create table
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """ CREATE TABLE ${tableName} (
                    k int,
                    c1 int,
                    c2 bitmap NOT NULL DEFAULT bitmap_empty,
                    c3 double DEFAULT PI,
                    c4 double DEFAULT E,
                    c5 array<int> NOT NULL DEFAULT "[]"
                    ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
                    PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                    "store_row_column" = "${use_row_store}"); """

            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${tableName}(k,c1) values(1,1),(2,2),(3,3);"
            sql "sync;"
            qt_sql "select k,c1,bitmap_to_string(c2),c3,c4,ARRAY_SIZE(c5) from ${tableName} order by k;"

            sql "insert into ${tableName}(k,c1) values(1,10),(2,20),(4,40),(5,50);"
            sql "sync;"
            qt_sql "select k,c1,bitmap_to_string(c2),c3,c4,ARRAY_SIZE(c5) from ${tableName} order by k;"
        }
    }
}
