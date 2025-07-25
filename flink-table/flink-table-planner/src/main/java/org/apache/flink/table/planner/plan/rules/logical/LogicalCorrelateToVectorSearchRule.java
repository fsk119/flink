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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.functions.sql.SqlVectorSearch;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalVectorSearch;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.immutables.value.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Value.Enclosing
public class LogicalCorrelateToVectorSearchRule
        extends RelRule<LogicalCorrelateToVectorSearchRule.Config> {

    public static LogicalCorrelateToVectorSearchRule INSTANCE = Config.DEFAULT.toRule();

    protected LogicalCorrelateToVectorSearchRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableFunctionScan functionCall = call.rel(2);
        RexNode expression = functionCall.getCall();
        if (!(expression instanceof RexCall)) {
            return false;
        }
        RexCall rexCall = (RexCall) expression;
        return rexCall.getOperator() instanceof SqlVectorSearch;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCorrelate rel = call.rel(0);

        RelNode left = call.rel(1);
        LogicalTableFunctionScan functionCall = call.rel(2);
        Decorrelator decorrelator = new Decorrelator(rel.getCorrelationId());
        RexCall rewrittenCall = (RexCall) functionCall.getCall().accept(decorrelator);

        // descriptor
        RexCall tableCall = (RexCall) rewrittenCall.getOperands().get(0);
        RexCall descriptorCall = (RexCall) rewrittenCall.getOperands().get(1);
        Map<String, Integer> column2Index = new HashMap<>();
        java.util.List<String> fieldNames = tableCall.getType().getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            column2Index.put(fieldNames.get(i), i);
        }
        List<Integer> searchColumns =
                descriptorCall.getOperands().stream()
                        .map(
                                operand -> {
                                    if (operand instanceof RexLiteral) {
                                        RexLiteral literal = (RexLiteral) operand;
                                        String fieldName = RexLiteral.stringValue(literal);
                                        Integer index = column2Index.get(fieldName);
                                        if (index == null) {
                                            throw new TableException(
                                                    String.format(
                                                            "Field %s is not found in input schema: %s.",
                                                            fieldName, tableCall.getType()));
                                        }
                                        return index;
                                    } else {
                                        throw new TableException(
                                                String.format(
                                                        "Unknown operand for descriptor operator: %s.",
                                                        operand));
                                    }
                                })
                        .collect(Collectors.toList());
        //
        RexNode queryColumns = rewrittenCall.getOperands().get(2);
        assert queryColumns instanceof RexInputRef;
        int queryIndex = ((RexInputRef) queryColumns).getIndex();

        // topK
        RexNode topK = rewrittenCall.getOperands().get(3);

        // config
        RexNode config = rewrittenCall.getOperands().get(4);

        call.transformTo(
                LogicalVectorSearch.create(
                        left,
                        functionCall.getInput(0),
                        rel.getJoinType(),
                        queryIndex,
                        searchColumns.get(0),
                        topK,
                        config));
    }

    @Value.Immutable
    public interface Config extends RelRule.Config {

        Config DEFAULT =
                ImmutableLogicalCorrelateToVectorSearchRule.Config.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(LogicalCorrelate.class)
                                                .inputs(
                                                        b1 -> b1.operand(RelNode.class).anyInputs(),
                                                        b2 ->
                                                                b2.operand(
                                                                                LogicalTableFunctionScan
                                                                                        .class)
                                                                        .anyInputs()))
                        .withDescription("LogicalCorrelateToVectorSearchRule");

        @Override
        default LogicalCorrelateToVectorSearchRule toRule() {
            return new LogicalCorrelateToVectorSearchRule(this);
        }
    }

    class Decorrelator extends RexShuttle {

        private final CorrelationId correlationId;

        public Decorrelator(CorrelationId correlationId) {
            this.correlationId = correlationId;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                final RexCorrelVariable var = (RexCorrelVariable) fieldAccess.getReferenceExpr();
                assert var.id.equals(correlationId);
                final RelDataTypeField field = fieldAccess.getField();
                return new RexInputRef(field.getIndex(), field.getType());
            } else {
                return super.visitFieldAccess(fieldAccess);
            }
        }
    }
}
