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

package org.apache.flink.table.planner.functions.utils;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.internal.ResultProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.functions.ProcedureResult;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class AdaptedResultProvider implements ResultProvider {

    ProcedureResult<?> result;
    DataStructureConverter<Object, Object> converter;
    RowDataToStringConverter toStringConverter;
    DataType outputType;

    public AdaptedResultProvider(
            ProcedureResult<?> result,
            DataStructureConverter<Object, Object> converter,
            RowDataToStringConverterImpl toStringConverter) {
        this.result = result;
        this.converter = converter;
        this.toStringConverter = toStringConverter;
    }

    @Override
    public ResultProvider setJobClient(JobClient jobClient) {
        return this;
    }

    @Override
    public CloseableIterator<RowData> toInternalIterator() {
        CloseableIterator<?> internal = result.getValue();
        return new CloseableIterator<RowData>() {

            @Override
            public boolean hasNext() {
                return internal.hasNext();
            }

            @Override
            public RowData next() {
                Object element = internal.next();
                return (RowData) converter.toInternal(element);
            }

            @Override
            public void close() throws Exception {
                internal.close();
            }
        };
    }

    @Override
    public CloseableIterator<Row> toExternalIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowDataToStringConverter getRowDataStringConverter() {
        return toStringConverter;
    }

    @Override
    public boolean isFirstRowReady() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
