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

package org.apache.flink.table.planner.plan.nodes.logical

import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalVectorSearch

import org.apache.calcite.plan.{Convention, RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.validate.SqlValidatorUtil

import java.util.Collections

class FlinkLogicalVectorSearch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    val joinType: JoinRelType,
    val queryColumn: Int,
    val searchColumn: Int,
    val topK: RexNode,
    val config: RexNode)
  extends BiRel(cluster, traitSet, left, right)
  with FlinkLogicalRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new FlinkLogicalVectorSearch(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      joinType,
      queryColumn,
      searchColumn,
      topK,
      config)
  }

  override def deriveRowType(): RelDataType = {
    joinType match {
      case JoinRelType.INNER | JoinRelType.LEFT =>
        SqlValidatorUtil.deriveJoinRowType(
          left.getRowType,
          right.getRowType,
          joinType,
          getCluster.getTypeFactory,
          null,
          Collections.emptyList())
      case _ =>
        throw new IllegalStateException("Unknown join type " + joinType)
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .item("joinType", joinType)
      .item("queryColumn", queryColumn)
      .item("searchColumn", searchColumn)
      .item("topK", topK)
      .item("config", config)
  }
}

class FlinkLogicalVectorSearchConverter(config: ConverterRule.Config)
  extends ConverterRule(config) {
  override def convert(rel: RelNode): RelNode = {
    val vectorSearch = rel.asInstanceOf[LogicalVectorSearch]

    val left = RelOptRule.convert(vectorSearch.getInput(0), FlinkConventions.LOGICAL)
    val right = RelOptRule.convert(vectorSearch.getInput(1), FlinkConventions.LOGICAL)

    FlinkLogicalVectorSearch.create(
      left,
      right,
      vectorSearch.joinType,
      vectorSearch.queryColumn,
      vectorSearch.searchColumn,
      vectorSearch.topK,
      vectorSearch.config)
  }
}

object FlinkLogicalVectorSearch {

  val CONVERTER = new FlinkLogicalVectorSearchConverter(
    Config.INSTANCE.withConversion(
      classOf[LogicalVectorSearch],
      Convention.NONE,
      FlinkConventions.LOGICAL,
      "FlinkLogicalVectorSearchConverter"))

  def create(
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      queryColumn: Int,
      searchColumn: Int,
      topK: RexNode,
      config: RexNode): FlinkLogicalVectorSearch = {
    val cluster = left.getCluster
    val traitSet = cluster.traitSet().replace(FlinkConventions.LOGICAL).simplify()
    new FlinkLogicalVectorSearch(
      cluster,
      traitSet,
      left,
      right,
      joinType,
      queryColumn,
      searchColumn,
      topK,
      config
    )

  }

}
