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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.table.gateway.api.results.ResultSet;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.rest.serde.RowDataUtil.toJsonRowDataInfo;

/** Describe result. */
public class ResultInfo {

    private final RowFormat rowFormat;
    private final List<ColumnInfo> columnInfo;
    private final List<RowDataInfo> rowDataInfo;

    public ResultInfo(
            RowFormat rowFormat, List<ColumnInfo> columnInfo, List<RowDataInfo> rowDataInfo) {
        this.rowFormat = rowFormat;
        this.columnInfo = columnInfo;
        this.rowDataInfo = rowDataInfo;
    }

    public List<ColumnInfo> getColumnInfo() {
        return columnInfo;
    }

    public List<RowDataInfo> getRowDataInfo() {
        return rowDataInfo;
    }

    public static ResultInfo create(ResultSet resultSet, RowFormat rowFormat) {
        List<ColumnInfo> columnInfos =
                resultSet.getResultSchema().getColumns().stream()
                        .map(
                                col ->
                                        new ColumnInfo(
                                                col.getName(),
                                                col.getDataType().getLogicalType(),
                                                col.getComment().orElse(null)))
                        .collect(Collectors.toList());
        List<RowDataInfo> rowDataInfos;
        if (rowFormat == RowFormat.JSON) {
            rowDataInfos = toJsonRowDataInfo(resultSet);
        } else if (rowFormat == RowFormat.PLAIN_TEXT) {
            rowDataInfos = createPlainTextRowDataInfo(resultSet);
        } else {
            throw new IllegalArgumentException(String.format("Unknown row format: %s.", rowFormat));
        }
        return new ResultInfo(rowFormat, columnInfos, rowDataInfos);
    }

    private static List<RowDataInfo> createPlainTextRowDataInfo(ResultSet resultSet) {
        throw new UnsupportedOperationException("Not implemented.");
    }
}
