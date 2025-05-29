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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.functions.sql.ml.SqlMLPredictTableFunction
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMLPredict

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rex.RexCall

/** Rule that converts [[FlinkLogicalTableFunctionScan]] to [[StreamPhysicalMLPredict]]. */
class StreamPhysicalMLPredictRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableFunctionScan = call.rel(0).asInstanceOf[FlinkLogicalTableFunctionScan]
    tableFunctionScan.getCall match {
      case expr: RexCall => expr.getOperator.isInstanceOf[SqlMLPredictTableFunction]
      case _ => false
    }
  }

  override def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[FlinkLogicalTableFunctionScan]
    val convInput: RelNode =
      RelOptRule.convert(scan.getInput(0), FlinkConventions.STREAM_PHYSICAL)
    new StreamPhysicalMLPredict(
      rel.getCluster,
      rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL),
      convInput,
      scan,
      rel.getRowType)
  }
}

object StreamPhysicalMLPredictRule {
  val INSTANCE: RelOptRule = new StreamPhysicalMLPredictRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalTableFunctionScan],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamPhysicalMLPredictRule"))
}
