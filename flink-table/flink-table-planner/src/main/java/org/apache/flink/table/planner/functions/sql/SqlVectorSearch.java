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

package org.apache.flink.table.planner.functions.sql;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.TableCharacteristic;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SqlVectorSearch extends SqlFunction implements SqlTableFunction {

    private static final String NAME = "VECTOR_SEARCH";

    private static final String PARAM_SEARCH_TABLE = "SEARCH_TABLE";
    private static final String PARAM_COLUMN_TO_SEARCH = "COLUMN_TO_SEARCH";
    private static final String PARAM_COLUMN_TO_QUERY = "COLUMN_TO_QUERY";
    private static final String PARAM_TOP_K = "TOP_K";
    private static final String PARAM_DISTANCE_TYPE = "DISTANCE_TYPE";
    private static final String PARAM_OPTIONS = "CONFIG";

    private final Map<Integer, TableCharacteristic> tableParams =
            ImmutableMap.of(
                    0,
                    TableCharacteristic.builder(TableCharacteristic.Semantics.ROW)
                            .pruneIfEmpty()
                            .passColumnsThrough()
                            .build());

    public SqlVectorSearch() {
        super(
                NAME,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.CURSOR,
                null,
                new OperandMetadataImpl(),
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public SqlReturnTypeInference getRowTypeInference() {
        return new SqlReturnTypeInference() {
            @Override
            public @Nullable RelDataType inferReturnType(SqlOperatorBinding sqlOperatorBinding) {
                return inferRowType(sqlOperatorBinding);
            }
        };
    }

    private static RelDataType inferRowType(SqlOperatorBinding opBinding) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType baseTableRowType = opBinding.getOperandType(0);
        return inferRowType(typeFactory, baseTableRowType);
    }

    public static RelDataType inferRowType(
            RelDataTypeFactory typeFactory, RelDataType baseTableRowType) {
        // TODO:Fix this if columns have same names
        return typeFactory
                .builder()
                .kind(baseTableRowType.getStructKind())
                .addAll(baseTableRowType.getFieldList())
                .build();
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }

    private static class OperandMetadataImpl implements SqlOperandMetadata {

        private static final List<String> PARAMETERS =
                Collections.unmodifiableList(
                        Arrays.asList(
                                PARAM_SEARCH_TABLE,
                                PARAM_COLUMN_TO_SEARCH,
                                PARAM_COLUMN_TO_QUERY,
                                PARAM_TOP_K,
                                PARAM_DISTANCE_TYPE,
                                PARAM_OPTIONS));

        @Override
        public List<RelDataType> paramTypes(RelDataTypeFactory relDataTypeFactory) {
            return Collections.nCopies(
                    PARAMETERS.size(), relDataTypeFactory.createSqlType(SqlTypeName.ANY));
        }

        @Override
        public List<String> paramNames() {
            return PARAMETERS;
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.between(3, 5);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return String.format("%s(input_name, index, query, options)", opName);
        }

        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }

        @Override
        public boolean isOptional(int i) {
            return i > 4;
        }
    }
}
