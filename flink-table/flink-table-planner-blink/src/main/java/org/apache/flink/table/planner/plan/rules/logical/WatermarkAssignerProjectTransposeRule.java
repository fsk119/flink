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

import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;

/**
 * WatermarkAssignerProjectTransposeRule.
 * */
public class WatermarkAssignerProjectTransposeRule extends RelOptRule {
	public static final WatermarkAssignerProjectTransposeRule INSTANCE = new WatermarkAssignerProjectTransposeRule();

	public WatermarkAssignerProjectTransposeRule() {
		super(operand(LogicalWatermarkAssigner.class,
				operand(LogicalProject.class,
						operand(LogicalTableScan.class, none()))),
				"WatermarkAssignerProjectTransposeRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalTableScan scan = call.rel(2);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		return tableSourceTable != null && tableSourceTable.tableSource() instanceof SupportsWatermarkPushDown;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		LogicalWatermarkAssigner watermarkAssigner = call.rel(0);
		LogicalProject project = call.rel(1);

		RexNode computedColumn = project.getProjects().get(watermarkAssigner.rowtimeFieldIndex());

		RexNode newWatermarkExpr = watermarkAssigner.watermarkExpr().accept(new RexShuttle() {
			@Override
			public RexNode visitInputRef(RexInputRef inputRef) {
				return computedColumn;
			}
		});

		// use -1 to indicate rowtime column is not in scan.
		LogicalWatermarkAssigner newWatermarkAssigner =
				(LogicalWatermarkAssigner) watermarkAssigner.copy(watermarkAssigner.getTraitSet(),
				project.getInput(),
				-1,
				newWatermarkExpr);

		List<RexNode> newProjections = new ArrayList<>(project.getProjects());
		FlinkTypeFactory typeFactory = (FlinkTypeFactory) watermarkAssigner.getCluster().getTypeFactory();
		RexBuilder builder = call.builder().getRexBuilder();

		// cast timestamp type to rowtime type.
		RexNode newRexNode = builder.makeReinterpretCast(
				typeFactory.createRowtimeIndicatorType(computedColumn.getType().isNullable()),
				newProjections.get(watermarkAssigner.rowtimeFieldIndex()),
				null);
		newProjections.set(watermarkAssigner.rowtimeFieldIndex(), newRexNode);
		LogicalProject newProject = project.copy(
				project.getTraitSet(),
				newWatermarkAssigner,
				newProjections,
				watermarkAssigner.getRowType());

		call.transformTo(newProject);
	}
}
