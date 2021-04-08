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

import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Pushes a {@link LogicalFilter} from the {@link LogicalCalc} and into a {@link LogicalTableScan}.
 */
public class PushFilterInCalcIntoTableSourceScanRule extends PushFilterIntoSourceScanRuleBase {
    public static final PushFilterInCalcIntoTableSourceScanRule INSTANCE =
            new PushFilterInCalcIntoTableSourceScanRule();

    public PushFilterInCalcIntoTableSourceScanRule() {
        super(
                operand(FlinkLogicalCalc.class, operand(FlinkLogicalTableSourceScan.class, none())),
                "PushFilterInCalcIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!super.matches(call)) {
            return false;
        }

        FlinkLogicalCalc calc = call.rel(0);
        RexProgram originProgram = calc.getProgram();

        if (originProgram.getCondition() == null) {
            return false;
        }

        FlinkLogicalTableSourceScan scan = call.rel(1);
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        // we can not push filter twice
        return canPushdownFilter(tableSourceTable);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        FlinkLogicalTableSourceScan scan = call.rel(1);
        TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
        pushFilterIntoScan(call, calc, scan, table);
    }

    private void pushFilterIntoScan(
            RelOptRuleCall call,
            Calc calc,
            FlinkLogicalTableSourceScan scan,
            FlinkPreparingTableBase relOptTable) {

        RexProgram originProgram = calc.getProgram();

        RelBuilder relBuilder = call.builder();
        Tuple2<RexNode[], RexNode[]> extractedPredicates =
                extractPredicates(
                        originProgram.getInputRowType().getFieldNames().toArray(new String[0]),
                        originProgram.expandLocalRef(originProgram.getCondition()),
                        scan,
                        relBuilder.getRexBuilder());

        RexNode[] convertiblePredicates = extractedPredicates._1;
        if (convertiblePredicates.length == 0) {
            // no condition can be translated to expression
            return;
        }

        Tuple2<SupportsFilterPushDown.Result, TableSourceTable> pushdownResultWithScan =
                createTableScanAfterPushdown(
                        convertiblePredicates,
                        relOptTable.unwrap(TableSourceTable.class),
                        scan,
                        relBuilder);

        SupportsFilterPushDown.Result result = pushdownResultWithScan._1;
        TableSourceTable tableSourceTable = pushdownResultWithScan._2;

        FlinkLogicalTableSourceScan newScan =
                FlinkLogicalTableSourceScan.create(scan.getCluster(), tableSourceTable);

        // calc is empty
        if (originProgram.getProjectList().isEmpty()
                && extractedPredicates._2().length == 0
                && result.getRemainingFilters().isEmpty()) {
            call.transformTo(newScan);
            return;
        }

        // build new calc program
        RexProgramBuilder programBuilder =
                new RexProgramBuilder(newScan.getRowType(), call.builder().getRexBuilder());

        if (!result.getRemainingFilters().isEmpty() || extractedPredicates._2.length != 0) {
            RexNode[] unconvertedPredicates = extractedPredicates._2;
            RexNode remainingCondition =
                    getRemainingConditions(relBuilder, result, unconvertedPredicates);

            programBuilder.addCondition(remainingCondition);
        }

        List<RexNode> projects =
                originProgram.getProjectList().stream()
                        .map(originProgram::expandLocalRef)
                        .collect(Collectors.toList());
        List<String> outputFieldNames = originProgram.getOutputRowType().getFieldNames();

        for (int i = 0; i < projects.size(); i++) {
            programBuilder.addProject(projects.get(i), outputFieldNames.get(i));
        }

        FlinkLogicalCalc newCalc = FlinkLogicalCalc.create(newScan, programBuilder.getProgram());
        call.transformTo(newCalc);
    }
}
