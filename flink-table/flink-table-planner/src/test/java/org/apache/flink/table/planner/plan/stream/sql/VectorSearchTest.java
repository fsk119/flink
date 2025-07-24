/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.stream.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VectorSearchTest extends TableTestBase {

    private TableTestUtil util;

    @BeforeEach
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());

        // Create test table
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING,\n"
                                + "  d DECIMAL(10, 3),\n"
                                + "  rowtime1 TIMESTAMP(3),\n"
                                + "  proctime1 as PROCTIME(),\n"
                                + "  e ARRAY<FLOAT>,\n"
                                + "  WATERMARK FOR rowtime1 AS rowtime1 - INTERVAL '1' SECOND\n"
                                + ") with (\n"
                                + "  'connector' = 'values'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE VectorTable (\n"
                                + "  a1 INT,\n"
                                + "  b1 BIGINT,\n"
                                + "  c1 STRING,\n"
                                + "  d1 DECIMAL(10, 3),\n"
                                + "  e1 ARRAY<FLOAT>,\n"
                                + "  rowtime TIMESTAMP(3)\n"
                                //                                + "  proctime as PROCTIME()\n"
                                + ") with (\n"
                                + "  'connector' = 'values'\n"
                                + ")");

        // Create test model
        util.tableEnv()
                .executeSql(
                        "CREATE MODEL MyModel\n"
                                + "INPUT (a INT, b BIGINT)\n"
                                + "OUTPUT(e STRING, f ARRAY<INT>)\n"
                                + "with (\n"
                                + "  'provider' = 'test-model',\n" // test model provider defined in
                                // TestModelProviderFactory in
                                // flink-table-common
                                + "  'endpoint' = 'someendpoint',\n"
                                + "  'task' = 'text_generation'\n"
                                + ")");
    }

    @Test
    void testRunVectorSearch1() {
        String sql =
                "SELECT * FROM MyTable, lateral TABLE(\n"
                        + "vector_search(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`a1`), MyTable.rowtime1, 10, 'cosine'"
                        + ")\n"
                        + ")";
        util.verifyRelPlan(sql);
    }

    @Test
    void testRunVectorSearch2() {
        String sql =
                "SELECT * FROM MyTable, lateral TABLE(\n"
                        + "vector_search(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`e1`), MyTable.e, 10, 'cosine'"
                        + ")\n"
                        + ")";
        util.verifyRelPlan(sql);
    }

    @Test
    void testRunVectorSearch3() {
        String sql =
                "SELECT p.a1 FROM MyTable, lateral TABLE(\n"
                        + "vector_search(\n"
                        + "    TABLE VectorTable, DESCRIPTOR(`e1`), MyTable.e, 10, 'cosine'"
                        + ")\n"
                        + ") as p";
        util.verifyRelPlan(sql);
    }
}
