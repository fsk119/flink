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

package org.apache.flink.table.planner.functions.bridging;

import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlIdentifier;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeChecker;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlOperandTypeInference;
import static org.apache.flink.table.planner.functions.bridging.BridgingUtils.createSqlReturnTypeInference;

public class BridgingProcedureSqlFunction extends SqlFunction {

    private final ContextResolvedFunction procedure;

    public BridgingProcedureSqlFunction(
            String name,
            SqlIdentifier sqlIdentifier,
            @Nullable SqlReturnTypeInference returnTypeInference,
            @Nullable SqlOperandTypeInference operandTypeInference,
            @Nullable SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory category,
            ContextResolvedFunction procedure) {
        super(
                name,
                sqlIdentifier,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                category);
        this.procedure = procedure;
    }

    /** Creates an instance of a aggregate function during translation. */
    public static BridgingProcedureSqlFunction of(
            DataTypeFactory dataTypeFactory, ContextResolvedFunction resolvedFunction) {
        final TypeInference typeInference =
                resolvedFunction.getDefinition().getTypeInference(dataTypeFactory);
        return new BridgingProcedureSqlFunction(
                BridgingUtils.createName(resolvedFunction),
                createSqlIdentifier(resolvedFunction),
                createSqlReturnTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeInference(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                createSqlOperandTypeChecker(
                        dataTypeFactory, resolvedFunction.getDefinition(), typeInference),
                SqlFunctionCategory.USER_DEFINED_PROCEDURE,
                resolvedFunction);
    }

    public ContextResolvedFunction getProcedure() {
        return procedure;
    }
}
