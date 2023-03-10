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

package org.apache.flink.table.planner.operations;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.functions.UserDefinedProcedure;
import org.apache.flink.table.operations.CallProcedureOperation;

import java.lang.reflect.Method;

public class PlannerCallOperation implements CallProcedureOperation {

    private final UserDefinedProcedure procedure;
    private final Method methodHandle;
    private final Object[] arguments;
    private final DataStructureConverter<Object, Object> converter;

    public PlannerCallOperation(
            UserDefinedProcedure procedure,
            Method methodHandle,
            Object[] arguments,
            DataStructureConverter<Object, Object> converter) {
        this.procedure = procedure;
        this.methodHandle = methodHandle;
        this.arguments = arguments;
        this.converter = converter;
    }

    @Override
    public UserDefinedProcedure getDefinition() {
        return procedure;
    }

    @Override
    public Method getMethod() {
        return methodHandle;
    }

    @Override
    public Object[] getArguments() {
        return arguments;
    }

    @Override
    public RowData toInternal(Object value) {
        converter.open(PlannerCallOperation.class.getClassLoader());
        Object internal = converter.toInternal(value);

        if (internal instanceof RowData) {
            return (RowData) internal;
        } else {
            return GenericRowData.of(internal);
        }
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
