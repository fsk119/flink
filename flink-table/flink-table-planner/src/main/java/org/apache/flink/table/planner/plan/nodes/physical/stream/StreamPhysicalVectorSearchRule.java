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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.planner.functions.sql.SqlVectorSearch;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.immutables.value.Value;

@Value.Enclosing
public class StreamPhysicalVectorSearchRule
        extends RelRule<StreamPhysicalVectorSearchRule.VectorSearchRuleConfig> {

    public static StreamPhysicalVectorSearchRule INSTANCE =
            new StreamPhysicalVectorSearchRule(VectorSearchRuleConfig.DEFAULT);

    protected StreamPhysicalVectorSearchRule(
            StreamPhysicalVectorSearchRule.VectorSearchRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalTableFunctionScan functionCall = call.rel(2);
        RexNode expression = functionCall.getCall();
        if (!(expression instanceof RexCall)) {
            return false;
        }
        RexCall rexCall = (RexCall) expression;
        return rexCall.getOperator() instanceof SqlVectorSearch;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCorrelate rel = call.rel(0);
        final RelNode newInput =
                RelOptRule.convert(call.rel(1), FlinkConventions.STREAM_PHYSICAL());
        FlinkLogicalTableFunctionScan functionCall = call.rel(2);
        FlinkLogicalTableSourceScan scan = call.rel(3);
        RelOptTable temporalTable = scan.getTable();
        // try to decorrelate the expression
        Decorrelator decorrelator = new Decorrelator(rel.getCorrelationId());

        FlinkLogicalTableFunctionScan rewrittenFunctionCall =
                (FlinkLogicalTableFunctionScan)
                        functionCall.copy(
                                functionCall.getTraitSet(),
                                functionCall.getInputs(),
                                functionCall.getCall().accept(decorrelator),
                                functionCall.getElementType(),
                                functionCall.getRowType(),
                                functionCall.getColumnMappings());

        call.transformTo(
                new StreamPhysicalVectorSearch(
                        rel.getCluster(),
                        rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL()),
                        newInput,
                        rewrittenFunctionCall,
                        temporalTable,
                        rel.getRowType(),
                        rel.getJoinType()));
    }

    @Value.Immutable
    public interface VectorSearchRuleConfig extends RelRule.Config {
        ImmutableStreamPhysicalVectorSearchRule.VectorSearchRuleConfig DEFAULT =
                ImmutableStreamPhysicalVectorSearchRule.VectorSearchRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCorrelate.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        b2 ->
                                                                b2.operand(
                                                                                FlinkLogicalTableFunctionScan
                                                                                        .class)
                                                                        .oneInput(
                                                                                b3 ->
                                                                                        b3.operand(
                                                                                                        FlinkLogicalTableSourceScan
                                                                                                                .class)
                                                                                                .anyInputs())))
                        .withDescription("StreamPhysicalVectorSearchRule");

        @Override
        default StreamPhysicalVectorSearchRule toRule() {
            return new StreamPhysicalVectorSearchRule(this);
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
