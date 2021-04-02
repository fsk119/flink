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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link PushFilterInCalcIntoTableSourceRuleTest}. */
public class PushFilterInCalcIntoTableSourceRuleTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(new TableConfig());

    @Before
    public void setup() {
        FlinkChainedProgram<StreamOptimizeContext> program = new FlinkChainedProgram<>();
        program.addLast(
                "Converters",
                FlinkVolcanoProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .add(
                                RuleSets.ofList(
                                        CoreRules.PROJECT_TO_CALC,
                                        CoreRules.FILTER_TO_CALC,
                                        FlinkLogicalCalc.CONVERTER(),
                                        FlinkLogicalTableSourceScan.CONVERTER(),
                                        FlinkLogicalWatermarkAssigner.CONVERTER()))
                        .setRequiredOutputTraits(new Convention[] {FlinkConventions.LOGICAL()})
                        .build());
        program.addLast(
                "PushWatermarkIntoTableSourceScanRule",
                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(
                                RuleSets.ofList(
                                        PushWatermarkIntoTableSourceScanRule.INSTANCE,
                                        PushWatermarkIntoTableSourceScanAcrossCalcRule.INSTANCE))
                        .build());
        program.addLast(
                "Fliter push down",
                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(RuleSets.ofList(PushFilterIntoTableSourceScanRule.INSTANCE))
                        .build());
        util.replaceStreamProgram(program);
    }

    @Test
    public void testFilterPushDownAfterWatermarkPushDown() {
        String ddl =
                "CREATE TABLE MTable (\n"
                        + "  a STRING,\n"
                        + "  b STRING,\n"
                        + "  c TIMESTAMP(3),\n"
                        + "  WATERMARK FOR c as c"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'a;b',\n"
                        + " 'bounded' = 'false',\n"
                        + " 'enable-watermark-push-down' = 'true',\n"
                        + "  'disable-lookup' = 'true'"
                        + ")";
        util.tableEnv().executeSql(ddl);
        util.verifyRelPlan("SELECT * FROM MTable WHERE LOWER(a) = 'foo' AND UPPER(b) = 'bar'");
    }
}
