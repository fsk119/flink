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

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RowDataUtil {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final RowDataToJsonConverters TO_JSON_CONVERTERS =
            new RowDataToJsonConverters(
                    TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.LITERAL, "null");
    private static final JsonToRowDataConverters TO_ROWDATA_CONVERTERS =
            new JsonToRowDataConverters(false, false, TimestampFormat.ISO_8601);

    public static List<RowDataInfo> toJsonRowDataInfo(ResultSet resultSet) {
        // The fieldGetters for all RowData
        List<RowData.FieldGetter> fieldGetters = buildFieldGetters(resultSet);
        // Generate converters for all fieldTypes
        List<RowDataToJsonConverters.RowDataToJsonConverter> converters =
                resultSet.getResultSchema().getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .map(TO_JSON_CONVERTERS::createConverter)
                        .collect(Collectors.toList());
        List<RowDataInfo> data = new ArrayList<>();
        for (RowData row : resultSet.getData()) {
            RowKind rowKind = row.getRowKind();
            List<String> fields = new ArrayList<>();
            for (int i = 0; i < row.getArity(); ++i) {
                Object field = fieldGetters.get(i).getFieldOrNull(row);
                RowDataToJsonConverters.RowDataToJsonConverter converter = converters.get(i);
                fields.add(buildJsonValueConverter(converter).apply(field));
            }
            data.add(new RowDataInfo(rowKind.name(), fields));
        }
        return data;
    }

    public static List<RowData> fromJsonRowDataInfo(ResultInfo resultInfo) {
        List<RowData> data = new ArrayList<>();
        List<JsonToRowDataConverters.JsonToRowDataConverter> converters =
                resultInfo.getColumnInfo().stream()
                        .map(ColumnInfo::getLogicalType)
                        .map(TO_ROWDATA_CONVERTERS::createConverter)
                        .collect(Collectors.toList());
        try {
            // Parse the RowData from RowDataInfo
            for (RowDataInfo rowDataInfo : resultInfo.getRowDataInfo()) {
                RowKind rowKind = RowKind.valueOf(rowDataInfo.getKind());
                GenericRowData rowData =
                        new GenericRowData(rowKind, rowDataInfo.getFields().size());
                List<String> fields = rowDataInfo.getFields();
                // Setting fields of one RowData
                for (int i = 0; i < rowData.getArity(); ++i) {
                    JsonNode jsonNode = OBJECT_MAPPER.readTree(fields.get(i));

                    JsonToRowDataConverters.JsonToRowDataConverter converter = converters.get(i);
                    Object object = buildObjectValueConverter(converter).apply(jsonNode);
                    if (object != null && object.toString().equals("null")) {
                        rowData.setField(i, new BinaryStringData(""));
                    } else {
                        rowData.setField(i, object);
                    }
                }
                data.add(rowData);
            }
            return data;
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize", e);
        }
    }

    private static List<RowData.FieldGetter> buildFieldGetters(ResultSet resultSet) {
        List<RowData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < resultSet.getResultSchema().getColumnCount(); i++) {
            fieldGetters.add(
                    RowData.createFieldGetter(
                            resultSet
                                    .getResultSchema()
                                    .getColumnDataTypes()
                                    .get(i)
                                    .getLogicalType(),
                            i));
        }
        return fieldGetters;
    }

    private static Function<Object, String> buildJsonValueConverter(
            RowDataToJsonConverters.RowDataToJsonConverter converter) {
        return field -> {
            return converter.convert(OBJECT_MAPPER, null, field).toString();
        };
    }

    private static Function<JsonNode, Object> buildObjectValueConverter(
            JsonToRowDataConverters.JsonToRowDataConverter converter) {
        return converter::convert;
    }
}
