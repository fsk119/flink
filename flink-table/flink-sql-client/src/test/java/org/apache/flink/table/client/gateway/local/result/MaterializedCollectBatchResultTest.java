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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.ClientResult;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.gateway.rest.serde.RowDataInfo;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MaterializedCollectBatchResult}. */
class MaterializedCollectBatchResultTest extends BaseMaterializedResultTest {

    @Test
    void testSnapshot() throws Exception {
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        new String[] {"f0", "f1"},
                        new DataType[] {DataTypes.STRING(), DataTypes.INT()});
        final RowDataInfoConverter rowConverter =
                buildRowDataInfoConverter(schema.toPhysicalRowDataType());

        try (TestMaterializedCollectBatchResult result =
                new TestMaterializedCollectBatchResult(
                        new ClientResult(true, schema, JobID.generate(), CloseableIterator.empty()),
                        Integer.MAX_VALUE,
                        createInternalBinaryRowDataConverter(schema.toPhysicalRowDataType()))) {

            result.isRetrieving = true;

            result.processRecord(toRowDataInfo("A", 1));
            result.processRecord(toRowDataInfo("B", 1));
            result.processRecord(toRowDataInfo("A", 1));
            result.processRecord(toRowDataInfo("C", 2));

            assertThat(result.snapshot(1)).isEqualTo(TypedResult.payload(4));

            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(1),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("B", 1)),
                    result.retrievePage(2),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(3),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("C", 2)),
                    result.retrievePage(4),
                    rowConverter);

            result.processRecord(toRowDataInfo("A", 1));

            assertThat(result.snapshot(1)).isEqualTo(TypedResult.payload(5));

            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(1),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("B", 1)),
                    result.retrievePage(2),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(3),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("C", 2)),
                    result.retrievePage(4),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(5),
                    rowConverter);
        }
    }

    @Test
    void testLimitedSnapshot() throws Exception {
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        new String[] {"f0", "f1"},
                        new DataType[] {DataTypes.STRING(), DataTypes.INT()});

        final RowDataInfoConverter rowConverter =
                buildRowDataInfoConverter(schema.toPhysicalRowDataType());
        try (TestMaterializedCollectBatchResult result =
                new TestMaterializedCollectBatchResult(
                        new ClientResult(true, schema, JobID.generate(), CloseableIterator.empty()),
                        2, // limit the materialized table to 2 rows
                        3,
                        createInternalBinaryRowDataConverter(
                                schema.toPhysicalRowDataType()))) { // with 3 rows overcommitment
            result.isRetrieving = true;

            result.processRecord(toRowDataInfo("D", 1));
            result.processRecord(toRowDataInfo("A", 1));
            result.processRecord(toRowDataInfo("B", 1));
            result.processRecord(toRowDataInfo("A", 1));

            assertRowEquals(
                    Arrays.asList(
                            null, null, Row.of("B", 1), Row.of("A", 1)), // two over-committed rows
                    result.getMaterializedTable(),
                    rowConverter);

            assertThat(result.snapshot(1)).isEqualTo(TypedResult.payload(2));

            assertRowEquals(
                    Collections.singletonList(Row.of("B", 1)),
                    result.retrievePage(1),
                    rowConverter);
            assertRowEquals(
                    Collections.singletonList(Row.of("A", 1)),
                    result.retrievePage(2),
                    rowConverter);

            result.processRecord(toRowDataInfo("C", 1));

            assertRowEquals(
                    Arrays.asList(Row.of("A", 1), Row.of("C", 1)), // limit clean up has taken place
                    result.getMaterializedTable(),
                    rowConverter);

            result.processRecord(toRowDataInfo("A", 1));

            assertRowEquals(
                    Arrays.asList(null, Row.of("C", 1), Row.of("A", 1)),
                    result.getMaterializedTable(),
                    rowConverter);
        }
    }

    private RowDataInfoConverter buildRowDataInfoConverter(DataType rowType) {
        RowRowConverter converter = RowRowConverter.create(rowType);
        converter.open(MaterializedCollectBatchResultTest.class.getClassLoader());
        RowDataToStringConverter toStringConverter =
                new RowDataToStringConverterImpl(
                        rowType,
                        DateTimeUtils.UTC_ZONE.toZoneId(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        return new RowDataInfoConverter() {
            @Override
            public RowDataInfo convert(Row row) {
                return new RowDataInfo(
                        row.getKind(),
                        Arrays.asList(toStringConverter.convert(converter.toInternal(row))));
            }
        };
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    private static class TestMaterializedCollectBatchResult extends MaterializedCollectBatchResult
            implements AutoCloseable {

        private final Function<Row, BinaryRowData> converter;

        public boolean isRetrieving;

        public TestMaterializedCollectBatchResult(
                ClientResult tableResult,
                int maxRowCount,
                int overcommitThreshold,
                Function<Row, BinaryRowData> converter) {
            super(tableResult, maxRowCount, overcommitThreshold);
            this.converter = converter;
        }

        public TestMaterializedCollectBatchResult(
                ClientResult tableResult, int maxRowCount, Function<Row, BinaryRowData> converter) {
            super(tableResult, maxRowCount);
            this.converter = converter;
        }

        @Override
        protected boolean isRetrieving() {
            return isRetrieving;
        }
    }
}
