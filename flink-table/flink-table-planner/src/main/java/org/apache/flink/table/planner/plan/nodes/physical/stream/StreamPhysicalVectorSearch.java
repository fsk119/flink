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
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlDescriptorOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class StreamPhysicalVectorSearch extends SingleRel implements StreamPhysicalRel {

    Optional<RexProgram> program;

    RelOptTable temporalTable;
    JoinRelType joinType;
    RelDataType outputRowType;
    int queryColumn;
    int searchColumn;
    RexNode topK;

    public StreamPhysicalVectorSearch(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelOptTable temporalTable,
            int queryColumn,
            int searchColumn,
            RexNode topK,
            JoinRelType joinRelType,
            RelDataType outputRowType) {
        super(cluster, traits, input);
        this.temporalTable = temporalTable;
        this.joinType = joinRelType;
        this.queryColumn = queryColumn;
        this.searchColumn = searchColumn;
        this.topK = topK;
        this.outputRowType = outputRowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalVectorSearch(
                getCluster(),
                traitSet,
                inputs.get(0),
                temporalTable,
                queryColumn,
                searchColumn,
                topK,
                joinType,
                outputRowType);
    }

    @Override
    protected RelDataType deriveRowType() {
        FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) getCluster().getTypeFactory();
        RelDataType rightType = temporalTable.getRowType();
        return SqlValidatorUtil.deriveJoinRowType(
                getInput(0).getRowType(),
                rightType,
                joinType,
                flinkTypeFactory,
                null,
                Collections.emptyList());
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        Map<Integer, FunctionCallUtil.FunctionParam> mappings =
                Collections.singletonMap(searchColumn, new FunctionCallUtil.FieldRef(queryColumn));
        FunctionCallUtil.FunctionParam topKParameter =
                new FunctionCallUtil.Constant(DataTypes.INT().getLogicalType(), (RexLiteral) topK);
        return new StreamExecVectorSearch(
                ShortcutUtils.unwrapTableConfig(this),
                JoinTypeUtil.getFlinkJoinType(joinType),
                new TemporalTableSourceSpec(temporalTable),
                topKParameter,
                mappings,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("joinType", joinType)
                .item("queryColumn", queryColumn)
                .item("searchColumn", searchColumn)
                .item("topK", topK);
    }
}
