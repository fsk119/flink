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
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ResultInfoSerializer extends StdSerializer<ResultInfo> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final RowDataToJsonConverters TO_JSON_CONVERTERS =
            new RowDataToJsonConverters(
                    TimestampFormat.ISO_8601,
                    JsonFormatOptions.MapNullKeyMode.LITERAL,
                    RowDataInfo.NULL_VALUE);

    public static final String FIELD_NAME_ROW_FORMAT = "format";
    public static final String FIELD_NAME_COLUMN = "columns";
    public static final String FIELD_NAME_DATA = "data";

    public static final String FIELD_NAME_KIND = "kind";
    public static final String FIELD_NAME_FIELDS = "fields";

    public ResultInfoSerializer() {
        super(ResultInfo.class);
    }

    @Override
    public void serialize(
            ResultInfo info, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        RowFormat rowFormat = info.getRowFormat();
        // serialize row format
        serializerProvider.defaultSerializeField(FIELD_NAME_ROW_FORMAT, rowFormat, jsonGenerator);
        // serialize column info
        serializerProvider.defaultSerializeField(
                FIELD_NAME_COLUMN, info.getColumnInfo(), jsonGenerator);
        // serialize data
        if (rowFormat == RowFormat.JSON) {
            serializeWithJsonFormat(info, jsonGenerator, serializerProvider);
        } else {
            throw new UnsupportedEncodingException("Not implemented.");
        }

        jsonGenerator.writeEndObject();
    }

    private void serializeWithJsonFormat(
            ResultInfo result, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        // The fieldGetters for all RowData
        List<RowData.FieldGetter> fieldGetters = new ArrayList<>();
        List<LogicalType> fieldTypes = new ArrayList<>();
        for (int i = 0; i < result.getColumnInfo().size(); i++) {
            ColumnInfo column = result.getColumnInfo().get(i);
            fieldGetters.add(RowData.createFieldGetter(column.getLogicalType(), i));
            fieldTypes.add(column.getLogicalType());
        }

        // Generate converters for all fieldTypes
        List<RowDataToJsonConverters.RowDataToJsonConverter> converters =
                fieldTypes.stream()
                        .map(TO_JSON_CONVERTERS::createConverter)
                        .collect(Collectors.toList());

        jsonGenerator.writeFieldName(FIELD_NAME_DATA);
        jsonGenerator.writeStartArray();
        for (RowDataInfo rowDataInfo : result.getRowDataInfo()) {
            RowData rowData = rowDataInfo.toRowData();
            serializeRowData(rowData, jsonGenerator, serializerProvider, converters, fieldGetters);
        }
        jsonGenerator.writeEndArray();
    }

    private void serializeRowData(
            RowData rowData,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider,
            List<RowDataToJsonConverters.RowDataToJsonConverter> converters,
            List<RowData.FieldGetter> fieldGetters)
            throws IOException {
        jsonGenerator.writeStartObject();
        // serialize row kind
        serializerProvider.defaultSerializeField(
                FIELD_NAME_KIND, rowData.getRowKind(), jsonGenerator);

        // serialize fields
        jsonGenerator.writeFieldName(FIELD_NAME_FIELDS);
        jsonGenerator.writeStartArray();
        for (int i = 0; i < rowData.getArity(); i++) {
            serializerProvider.defaultSerializeValue(
                    converters.get(i).convert(OBJECT_MAPPER, null, fieldGetters.get(i)),
                    jsonGenerator);
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject();
    }
}
