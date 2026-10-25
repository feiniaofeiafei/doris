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

suite("test_view_definition_binding") {
    sql "DROP VIEW IF EXISTS view_binding_repeated"
    sql "DROP VIEW IF EXISTS view_binding_except"
    sql "DROP VIEW IF EXISTS view_binding_replace"
    sql "DROP VIEW IF EXISTS view_binding_variant"
    sql "DROP VIEW IF EXISTS view_binding_aggregate_star"
    sql "DROP VIEW IF EXISTS view_binding_group_alias"
    sql "DROP VIEW IF EXISTS view_binding_implicit_name"
    sql "DROP VIEW IF EXISTS view_binding_after_failure"
    sql "DROP TABLE IF EXISTS view_binding_source"

    sql """
        CREATE TABLE view_binding_source (
            k INT,
            v INT,
            payload VARIANT<'video_id':bigint, 'duration':bigint>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "storage_format" = "V3")
    """
    sql """
        INSERT INTO view_binding_source VALUES
            (1, 10, parse_to_variant('{"video_id":100,"duration":5}')),
            (1, 20, parse_to_variant('{"video_id":101,"duration":7}')),
            (2, 30, parse_to_variant('{"video_id":200,"duration":9}')),
            (3, NULL, parse_to_variant('{"video_id":300,"duration":11}'))
    """

    // Every occurrence of a repeated column is rewritten in its own source location.
    sql """
        CREATE VIEW view_binding_repeated AS
        SELECT s.k AS original_k, s.k + 1 AS next_k, s.v
        FROM view_binding_source s WHERE s.k > 0 AND s.v > s.k
    """
    order_qt_repeated "SELECT * FROM view_binding_repeated"

    // A star owns EXCEPT/REPLACE, while the neighboring projection remains independent.
    sql """
        CREATE VIEW view_binding_except AS
        SELECT * EXCEPT (payload), k + v AS total FROM view_binding_source
    """
    order_qt_except_projection "SELECT * FROM view_binding_except"
    sql """
        ALTER VIEW view_binding_except AS
        SELECT * EXCEPT (v, payload), sum(v) AS total FROM view_binding_source GROUP BY k
    """
    order_qt_except_aggregate "SELECT * FROM view_binding_except"

    sql """
        CREATE VIEW view_binding_replace AS
        SELECT * REPLACE ((v + 1) * 2 AS v) EXCEPT (payload), k + 3 AS next_k
        FROM view_binding_source
    """
    order_qt_replace_source """
        SELECT k, (v + 1) * 2 AS v, k + 3 AS next_k FROM view_binding_source
    """
    order_qt_replace_view "SELECT * FROM view_binding_replace"

    // Qualifying the base column must preserve both dotted and bracket field access.
    sql """
        CREATE VIEW view_binding_variant AS
        SELECT k, CAST(s.payload.video_id AS BIGINT) AS dotted_id,
            CAST(s.payload['video_id'] AS BIGINT) AS bracket_id
        FROM view_binding_source s
    """
    order_qt_variant_create "SELECT * FROM view_binding_variant"
    sql """
        ALTER VIEW view_binding_variant AS
        SELECT k, CAST(payload.duration AS BIGINT) + 1 AS duration,
            CAST(payload['duration'] AS BIGINT) + 1 AS bracket_duration
        FROM view_binding_source
    """
    order_qt_variant_alter "SELECT * FROM view_binding_variant"

    // Expanding q.* cannot transfer q's qualifier to the aggregate's input slots.
    sql """
        CREATE VIEW view_binding_aggregate_star AS
        WITH source_rows AS (SELECT k, v FROM view_binding_source)
        SELECT q.*, q.total + 1 AS next_total
        FROM (SELECT k, sum(v) AS total FROM source_rows GROUP BY k) q
    """
    order_qt_aggregate_star "SELECT * FROM view_binding_aggregate_star"

    sql """
        CREATE VIEW view_binding_group_alias AS
        SELECT k + 1 AS grouping_key, sum(v) AS total FROM view_binding_source
        GROUP BY grouping_key HAVING total > 0 ORDER BY total
    """
    order_qt_group_alias "SELECT * FROM view_binding_group_alias"

    // References inside the definition use the original implicit name before schema naming.
    sql """
        CREATE VIEW view_binding_implicit_name AS
        WITH q AS (SELECT k + 1 FROM view_binding_source ORDER BY `k + 1` LIMIT 3)
        SELECT q.`k + 1` FROM q ORDER BY q.`k + 1` LIMIT 2
    """
    order_qt_implicit_name "SELECT * FROM view_binding_implicit_name"

    test {
        sql """
            CREATE VIEW view_binding_after_failure AS
            SELECT k, missing_column FROM view_binding_source
        """
        exception "Unknown column 'missing_column'"
    }
    sql """
        CREATE VIEW view_binding_after_failure AS
        SELECT k, v FROM view_binding_source
    """
    order_qt_after_failed_create "SELECT * FROM view_binding_after_failure"
    sql """
        ALTER VIEW view_binding_after_failure AS
        SELECT k, v + 1 AS next_v FROM view_binding_source
    """
    order_qt_after_failed_create_alter "SELECT * FROM view_binding_after_failure"
}
