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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class VectorSearchITCase extends StreamingTestBase {

    private final List<Row> data =
            Arrays.asList(
                    Row.of(1L, new Float[] {1.0f, 2.0f, 3.0f}),
                    Row.of(2L, new Float[] {1.0f, 2.0f, 3.0f}));

    @BeforeEach
    void beforeEach() {
        String dataId = TestValuesTableFactory.registerData(data);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE `%s`(\n"
                                        + "  id BIGINT,\n"
                                        + "  vec ARRAY<FLOAT>\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s'\n"
                                        + ")",
                                "src", dataId));

        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE `%s`(\n"
                                        + "  id BIGINT,\n"
                                        + "  vec ARRAY<FLOAT>\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values',\n"
                                        + "  'data-id' = '%s',\n"
                                        + "  'enable-search' = 'true'"
                                        + ")",
                                "vec", dataId));
    }

    @Test
    void test() {
        tEnv().executeSql(
                        "SELECT * FROM src, LATERAL TABLE(VECTOR_SEARCH(TABLE vec, DESCRIPTOR(`vec`), src.vec, 10))")
                .print();
    }
}
