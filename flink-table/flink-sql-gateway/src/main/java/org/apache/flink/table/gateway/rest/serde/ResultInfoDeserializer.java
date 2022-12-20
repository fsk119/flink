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
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_COLUMN_INFOS;
import static org.apache.flink.table.gateway.rest.serde.ResultInfoSerializer.FIELD_NAME_DATA;
import static org.apache.flink.table.gateway.rest.serde.ResultInfoSerializer.FIELD_NAME_FIELDS;
import static org.apache.flink.table.gateway.rest.serde.ResultInfoSerializer.FIELD_NAME_KIND;
import static org.apache.flink.table.gateway.rest.serde.ResultInfoSerializer.FIELD_NAME_ROW_FORMAT;

public class ResultInfoDeserializer extends StdDeserializer<ResultInfo> {

    private static final JsonToRowDataConverters TO_ROWDATA_CONVERTERS =
            new JsonToRowDataConverters(false, false, TimestampFormat.ISO_8601);

    public ResultInfoDeserializer() {
        super(ResultInfo.class);
    }

    @Override
    public ResultInfo deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // TODO: fix when row format doesn't exist
        RowFormat format =
                jsonParser.getCodec().treeToValue(node.get(FIELD_NAME_ROW_FORMAT), RowFormat.class);
        // Deserialize column infos
        List<ColumnInfo> columnInfos =
                Arrays.asList(
                        jsonParser
                                .getCodec()
                                .treeToValue(
                                        node.get(FIELD_NAME_COLUMN_INFOS), ColumnInfo[].class));
        // Deserialize data
        List<RowDataInfo> rows = deserializeDataWithJsonFormat(jsonParser, node, columnInfos);
        return new ResultInfo(format, columnInfos, rows);
    }

    private List<RowDataInfo> deserializeDataWithJsonFormat(
            JsonParser parser, JsonNode node, List<ColumnInfo> columnInfos) throws IOException {
        JsonNode[] jsonRows =
                parser.getCodec().treeToValue(node.get(FIELD_NAME_DATA), JsonNode[].class);

        List<JsonToRowDataConverters.JsonToRowDataConverter> converters =
                columnInfos.stream()
                        .map(col -> TO_ROWDATA_CONVERTERS.createConverter(col.getLogicalType()))
                        .collect(Collectors.toList());
        List<RowDataInfo> rows = new ArrayList<>();
        for (JsonNode jsonRow : jsonRows) {
            rows.add(deserializeRowDataInfoWithJsonFormat(parser, jsonRow, converters));
        }
        return rows;
    }

    private RowDataInfo deserializeRowDataInfoWithJsonFormat(
            JsonParser jsonParser,
            JsonNode node,
            List<JsonToRowDataConverters.JsonToRowDataConverter> converters)
            throws IOException {
        RowKind rowKind =
                jsonParser.getCodec().treeToValue(node.get(FIELD_NAME_KIND), RowKind.class);
        JsonNode[] jsonFields =
                jsonParser.getCodec().treeToValue(node.get(FIELD_NAME_FIELDS), JsonNode[].class);
        List<Object> fields = new ArrayList<>();
        for (int i = 0; i < converters.size(); i++) {
            fields.add(converters.get(i).convert(jsonFields[i]));
        }
        return new RowDataInfo(rowKind, fields);
    }
}
