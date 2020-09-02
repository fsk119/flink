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
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Test rule PushWatermarkIntoTableSourceScanRule.
 * */
public class PushWatermarkIntoTableSourceScanRuleTest extends TableTestBase {
	private StreamTableTestUtil util = streamTestUtil(new TableConfig());

	@Before
	public void setup() {
		util.buildStreamProgram(FlinkStreamProgram.DEFAULT_REWRITE());
		CalciteConfig calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv().getConfig());
		calciteConfig.getStreamProgram().get().addLast(
				"PushWatermarkIntoTableSourceScanRule",
				FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
						.setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
						.setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
						.add(RuleSets.ofList(
								WatermarkAssignerProjectTransposeRule.INSTANCE,
								PushWatermarkIntoTableSourceScanRule.INSTANCE))
						.build()
		);
	}

	@Test
	public void testSimpleWatermark() {
		String ddl = "create table MyTable(" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c timestamp(3),\n" +
				"  watermark for c as c - interval '5' second\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'false'\n" +
				")";
		util.tableEnv().executeSql(ddl);
		util.verifyPlan("select * from MyTable");
	}

	@Test
	public void testSimpleTranspose() {
		String ddl = "create table MyTable(" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c timestamp(3),\n" +
				"  d as c + interval '5' second,\n" +
				"  watermark for d as d - interval '5' second\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'false'\n" +
				")";
		util.tableEnv().executeSql(ddl);
		util.verifyPlan("select * from MyTable");
	}

	@Test
	public void testSimpleTransposeNotNull() {
		String ddl = "create table MyTable(" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c timestamp(3) not null,\n" +
				"  d as c + interval '5' second,\n" +
				"  watermark for d as d - interval '5' second\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'false'\n" +
				")";
		util.tableEnv().executeSql(ddl);
		util.verifyPlan("select * from MyTable");
	}

	@Test
	public void testComputedColumnWithMultipleInputs() {
		String ddl = "create table MyTable(" +
				"  a string,\n" +
				"  b string,\n" +
				"  c as to_timestamp(a, b),\n" +
				"  watermark for c as c - interval '5' second\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'false'\n" +
				")";
		util.tableEnv().executeSql(ddl);
		util.verifyPlan("select * from MyTable");
	}

	@Test
	public void testTransposeWithRow() {
		String ddl = "create table MyTable(" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c row<name string, d timestamp(3)>," +
				"  e as c.d," +
				"  watermark for e as e - interval '5' second\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'false'\n" +
				")";
		util.tableEnv().executeSql(ddl);
		util.verifyPlan("select * from MyTable");
	}

	@Test
	public void testTransposeWithNestedRow() {
		String ddl = "create table MyTable(" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c row<name string, d row<e string, f timestamp(3)>>," +
				"  g as c.d.f," +
				"  watermark for g as g - interval '5' second\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'false'\n" +
				")";
		util.tableEnv().executeSql(ddl);
		util.verifyPlan("select * from MyTable");
	}

	@Test
	public void testTransposeWithUdf() {
		util.addFunction("func1", new InnerUdf());
		String ddl = "create table MyTable(" +
				"  a int,\n" +
				"  b bigint,\n" +
				"  c timestamp(3)," +
				"  d as func1(c)," +
				"  watermark for d as d - interval '5' second\n" +
				") WITH (\n" +
				" 'connector' = 'values',\n" +
				" 'bounded' = 'false'\n" +
				")";
		util.tableEnv().executeSql(ddl);
		util.verifyPlan("select * from MyTable");
	}

	/**
	 * Udf for test.
	 * */
	public static class InnerUdf extends ScalarFunction {
		public LocalDateTime eval(LocalDateTime input) {
			return LocalDateTime.ofInstant(input.toInstant(ZoneOffset.UTC).plusMillis(5000), ZoneOffset.UTC);
		}
	}
}
