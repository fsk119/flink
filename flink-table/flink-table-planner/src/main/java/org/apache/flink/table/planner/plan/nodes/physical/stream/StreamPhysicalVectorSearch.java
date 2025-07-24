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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecVectorSearch;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.JoinTypeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlDescriptorOperator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StreamPhysicalVectorSearch extends StreamPhysicalCorrelateBase
        implements StreamPhysicalRel {

    Optional<RexProgram> program;

    RelOptTable temporalTable;
    RexCall descriptor;
    JoinRelType joinType;

    public StreamPhysicalVectorSearch(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            FlinkLogicalTableFunctionScan scan,
            RelOptTable temporalTable,
            RelDataType outputRowType,
            JoinRelType joinRelType) {
        super(
                cluster,
                traits,
                input,
                scan,
                JavaScalaConversionUtil.toScala(Optional.empty()),
                outputRowType,
                joinRelType);
        this.temporalTable = temporalTable;
        this.joinType = joinRelType;
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, RelNode newChild, RelDataType outputType) {
        return new StreamPhysicalVectorSearch(
                getCluster(), traitSet, newChild, scan(), temporalTable, outputType, joinType);
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        RexTableArgCall tableCall = extractOperand(operand -> operand instanceof RexTableArgCall);
        RexCall descriptorCall =
                extractOperand(
                        operand ->
                                operand instanceof RexCall
                                        && ((RexCall) operand).getOperator()
                                                instanceof SqlDescriptorOperator);
        Map<String, Integer> column2Index = new HashMap<>();
        java.util.List<String> fieldNames = tableCall.getType().getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            column2Index.put(fieldNames.get(i), i);
        }
        List<Integer> referenceKeys =
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
        assert referenceKeys.size() == 1;

        RexInputRef fieldAccess = extractOperand(operand -> operand instanceof RexInputRef);
        int queryColumn = fieldAccess.getIndex();

        Map<Integer, FunctionCallUtil.FunctionParam> mappings =
                Collections.singletonMap(
                        referenceKeys.get(0), new FunctionCallUtil.FieldRef(queryColumn));
        FunctionCallUtil.FunctionParam topK =
                new FunctionCallUtil.Constant(
                        DataTypes.INT().getLogicalType(),
                        extractOperand(operand -> operand instanceof RexLiteral));

        return new StreamExecVectorSearch(
                ShortcutUtils.unwrapTableConfig(this),
                JoinTypeUtil.getFlinkJoinType(joinType),
                new TemporalTableSourceSpec(temporalTable),
                topK,
                mappings,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        Optional<RexNode> condition = JavaScalaConversionUtil.toJava(condition());
        return pw.input("input", getInput())
                .item("invocation", scan().getCall())
                .item(
                        "table",
                        ((TableSourceTable) temporalTable)
                                .contextResolvedTable()
                                .getIdentifier()
                                .asSummaryString())
                .item("select", String.join(",", getRowType().getFieldNames()))
                .item("rowType", getRowType())
                .item("joinType", joinType)
                .itemIf("condition", condition.orElse(null), condition.isPresent());
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<T> extractOptionalOperand(Predicate<RexNode> predicate) {
        return (Optional<T>)
                ((RexCall) scan().getCall()).getOperands().stream().filter(predicate).findFirst();
    }

    @SuppressWarnings("unchecked")
    private <T> T extractOperand(Predicate<RexNode> predicate) {
        return (T)
                extractOptionalOperand(predicate)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                String.format(
                                                        "VectorSearch doesn't contain specified operand: %s",
                                                        scan().getCall().toString())));
    }
}
