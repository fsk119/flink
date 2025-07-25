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
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalVectorSearch;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.TemporalTable;
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
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalVectorSearch rel = call.rel(0);
        final RelNode newInput =
                RelOptRule.convert(call.rel(1), FlinkConventions.STREAM_PHYSICAL());
        RelNode right = call.rel(2);
        UnsupportedStructureFinder finder = new UnsupportedStructureFinder();
        finder.visit(right);

        call.transformTo(
                new StreamPhysicalVectorSearch(
                        rel.getCluster(),
                        rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL()),
                        newInput,
                        finder.temporalTable,
                        rel.queryColumn(),
                        rel.searchColumn(),
                        rel.topK(),
                        rel.joinType(),
                        rel.getRowType()));
    }

    @Value.Immutable
    public interface VectorSearchRuleConfig extends RelRule.Config {
        ImmutableStreamPhysicalVectorSearchRule.VectorSearchRuleConfig DEFAULT =
                ImmutableStreamPhysicalVectorSearchRule.VectorSearchRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalVectorSearch.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        b2 ->
                                                                b2.operand(FlinkLogicalRel.class)
                                                                        .anyInputs()))
                        .withDescription("StreamPhysicalVectorSearchRule");

        @Override
        default StreamPhysicalVectorSearchRule toRule() {
            return new StreamPhysicalVectorSearchRule(this);
        }
    }

    // Only support watermark assigner -> project -> scan
    // project -> scan or
    // scan
    // watermark -> scan
    // TODO: extract filter and projection later.
    static class UnsupportedStructureFinder {

        RelOptTable temporalTable;

        enum Node {
            WATERMARK_ASSIGNER,
            CALC,
            SCAN
        }

        private Node paranetNode;

        private void visit(RelNode p) {
            if (p instanceof RelSubset) {
                visitSubset((RelSubset) p);
            } else {
                visitRel(p);
            }
        }

        private void visitSubset(RelSubset subset) {
            RelNode cheapestOrOriginal = subset.getBestOrOriginal();
            visitRel(cheapestOrOriginal);
        }

        private Node transform(RelNode node) {
            Node transformed;
            if (node instanceof WatermarkAssigner) {
                transformed = Node.WATERMARK_ASSIGNER;
            } else if (node instanceof Calc) {
                transformed = Node.CALC;
            } else if (node instanceof TableScan) {
                transformed = Node.SCAN;
            } else {
                throw new RelOptPlanner.CannotPlanException(
                        String.format(
                                "Don't support %s node in the right tree.",
                                node.getClass().getSimpleName()));
            }
            return transformed;
        }

        /** Returns true when input {@code RelNode} is cyclic. */
        private void visitRel(RelNode p) {
            Node currentNode = transform(p);
            switch (currentNode) {
                case WATERMARK_ASSIGNER:
                    if (paranetNode != null) {
                        throw new RelOptPlanner.CannotPlanException(
                                String.format(
                                        "Assume watermark assigner is first node but it has parent %s.",
                                        paranetNode));
                    }
                    break;
                case CALC:
                    if (paranetNode == null || paranetNode == Node.WATERMARK_ASSIGNER) {
                        throw new RelOptPlanner.CannotPlanException(
                                String.format(
                                        "Assume calc is first node or calc is the first node but it has parent %s.",
                                        paranetNode));
                    }
                    break;
                case SCAN:
                    temporalTable = ((TableScan) p).getTable().unwrap(RelOptTable.class);
            }
            paranetNode = currentNode;
            if (currentNode != Node.SCAN) {
                visit(p.getInput(0));
            }
        }
    }
}
