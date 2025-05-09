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

suite("test_backup_restore_index") {

    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_backup_restore")
        return
    }
    
    def insert_data = { tableName ->
        [
            """INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100);""",
            """INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99);""",
            """INSERT INTO ${tableName} VALUES (2, "andy", "andy love apple", 100);""",
            """INSERT INTO ${tableName} VALUES (2, "bason", "bason hate pear", 99);""",
            """INSERT INTO ${tableName} VALUES (3, "andy", "andy love apple", 100);""",
            """INSERT INTO ${tableName} VALUES (3, "bason", "bason hate pear", 99);"""
        ]
    }

    def sqls = { tableName ->
        [
            """ select * from ${tableName} order by id, name, hobbies, score """,
            """ select * from ${tableName} where name match "andy" order by id, name, hobbies, score """,
            """ select * from ${tableName} where hobbies match "pear" order by id, name, hobbies, score """,
            """ select * from ${tableName} where score < 100 order by id, name, hobbies, score """
        ]
    }

    def run_sql = { tableName -> 
        sqls(tableName).each { sqlStatement ->
            def target_res = target_sql sqlStatement
            def res = sql sqlStatement
            assertEquals(res, target_res)
        }
    }

    def create_table_v1 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("support_phrase" = "true", "parser" = "english", "lower_case" = "true"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1",
            "binlog.enable" = "true"
        );
        """
    }

    def create_table_v2 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("support_phrase" = "true", "parser" = "english", "lower_case" = "true"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1", 
            "binlog.enable" = "true",
            "inverted_index_storage_format" = "V2"
        );
        """
    }

    def create_table_mow_v1 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("support_phrase" = "true", "parser" = "english", "lower_case" = "true"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1", 
            "binlog.enable" = "true", 
            "enable_unique_key_merge_on_write" = "true");
        """
    }

    def create_table_mow_v2 = { tableName ->
        """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL,
            index index_name (name) using inverted,
            index index_hobbies (hobbies) using inverted properties("support_phrase" = "true", "parser" = "english", "lower_case" = "true"),
            index index_score (score) using inverted
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( 
            "replication_num" = "1", 
            "binlog.enable" = "true", 
            "enable_unique_key_merge_on_write" = "true",
            "inverted_index_storage_format" = "V2");
        """
    }

    def run_test = { create_table, tableName -> 
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql create_table
        
        logger.info("=== Test 1: Common backup and restore ===")
        def snapshotName = "snapshot_test_" + tableName
        insert_data(tableName).each { sqlStatement ->
            sql sqlStatement
        }
        sql " sync "
        def res = sql "SELECT * FROM ${tableName}"
        if (tableName.contains("mow")) {
            assertEquals(res.size(), insert_data(tableName).size() / 2 as Integer)
        } else {
            assertEquals(res.size(), insert_data(tableName).size())
        }

        sql """ 
                BACKUP SNAPSHOT ${context.dbName}.${snapshotName} 
                TO `__keep_on_local__` 
                ON (${tableName})
                PROPERTIES ("type" = "full")
            """
        syncer.waitSnapshotFinish()
        assertTrue(syncer.getSnapshot("${snapshotName}", "${tableName}"))
        assertTrue(syncer.restoreSnapshot(true))
        syncer.waitTargetRestoreFinish()
        target_sql " sync "
        res = target_sql "SELECT * FROM ${tableName}"
        if (tableName.contains("mow")) {
            assertEquals(res.size(), insert_data(tableName).size() / 2 as Integer)
        } else {
            assertEquals(res.size(), insert_data(tableName).size())
        }
        run_sql(tableName)
    }

    // inverted index format v1
    logger.info("=== Test 1: Inverted index format v1 case ===")
    def tableName = "tbl_backup_restore_index_v1"
    run_test.call(create_table_v1(tableName), tableName)
    
    // inverted index format v2
    logger.info("=== Test 2: Inverted index format v2 case ===")
    tableName = "tbl_backup_restore_index_v2"
    run_test.call(create_table_v2(tableName), tableName)

    // inverted index format v1 with mow
    logger.info("=== Test 3: Inverted index format v1 with mow case ===")
    tableName = "tbl_backup_restore_index_mow_v1"
    run_test.call(create_table_mow_v1(tableName), tableName)

    // inverted index format v2 with mow
    logger.info("=== Test 4: Inverted index format v2 with mow case ===")
    tableName = "tbl_backup_restore_index_mow_v2"
    run_test.call(create_table_mow_v2(tableName), tableName)
}