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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.WatermarkGeneratorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration;

/**
 * Rule for PushWatermarkIntoTableSourceScan.
 * */
public class PushWatermarkIntoTableSourceScanRule extends RelOptRule {
	public static final PushWatermarkIntoTableSourceScanRule INSTANCE = new PushWatermarkIntoTableSourceScanRule();

	public PushWatermarkIntoTableSourceScanRule() {
		super(operand(LogicalWatermarkAssigner.class,
				operand(LogicalTableScan.class, none())),
				"PushWatermarkIntoTableSourceScanRule");
	}

	@Override
	public boolean matches(RelOptRuleCall call) {
		LogicalTableScan scan = call.rel(1);
		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		return tableSourceTable != null && tableSourceTable.tableSource() instanceof SupportsWatermarkPushDown;
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		LogicalWatermarkAssigner watermarkAssigner = call.rel(0);
		LogicalTableScan scan = call.rel(1);
		FlinkContext context = (FlinkContext) call.getPlanner().getContext();
		TableConfig config = context.getTableConfig();

		// generate inner watermark generator class that allows us to pass FunctionContext and ClassLoader
		GeneratedWatermarkGenerator generatedWatermarkGenerator =
				WatermarkGeneratorCodeGenerator.generateWatermarkGenerator(
						config,
						FlinkTypeFactory.toLogicalRowType(scan.getRowType()),
						watermarkAssigner.watermarkExpr(),
						"context",
						"classLoader"
				);
		Configuration configuration = context.getTableConfig().getConfiguration();

		WatermarkGeneratorSupplier<RowData> supplier = new DefaultWatermarkGeneratorSupplier(configuration, generatedWatermarkGenerator);
		String digest = String.format("watermark=[%s]", watermarkAssigner.watermarkExpr());

		WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.forGenerator(supplier);
		Long idleTimeout = getMillisecondFromConfigDuration(config, ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT);
		if (idleTimeout != null && idleTimeout > 0) {
			watermarkStrategy.withIdleness(Duration.ofMillis(idleTimeout));
			digest = String.format("%s idletimeout=[%s]", digest, idleTimeout);
		}

		TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
		DynamicTableSource newDynamicTableSource = tableSourceTable.tableSource().copy();

		((SupportsWatermarkPushDown) newDynamicTableSource).applyWatermark(watermarkStrategy);

		TableSourceTable newTableSourceTable = tableSourceTable.copy(
				newDynamicTableSource,
				watermarkAssigner.getRowType(),
				new String[]{digest});
		LogicalTableScan newScan = new LogicalTableScan(
				scan.getCluster(), scan.getTraitSet(), scan.getHints(), newTableSourceTable);

		call.transformTo(newScan);
	}

	private static class DefaultWatermarkGeneratorSupplier implements WatermarkGeneratorSupplier<RowData> {
		private final Configuration configuration;
		private final GeneratedWatermarkGenerator generatedWatermarkGenerator;

		public DefaultWatermarkGeneratorSupplier(Configuration configuration, GeneratedWatermarkGenerator generatedWatermarkGenerator) {
			this.configuration = configuration;
			this.generatedWatermarkGenerator = generatedWatermarkGenerator;
		}

		@Override
		public WatermarkGenerator<RowData> createWatermarkGenerator(Context context) {
			FunctionContext wrapper = new FunctionContextWrapper(context);

			List<Object> references = new ArrayList<>(Arrays.asList(generatedWatermarkGenerator.getReferences()));
			references.add(wrapper);
			references.add(Thread.currentThread().getContextClassLoader());

			org.apache.flink.table.runtime.generated.WatermarkGenerator innerWatermarkGenerator =
					new GeneratedWatermarkGenerator(
						generatedWatermarkGenerator.getClassName(),
						generatedWatermarkGenerator.getCode(),
						references.toArray())
							.newInstance(Thread.currentThread().getContextClassLoader());

			try {
				innerWatermarkGenerator.open(configuration);
			} catch (Exception e) {
				throw new RuntimeException("Fail to instantiate generated watermark generator.", e);
			}
			return new DefaultWatermarkGenerator(innerWatermarkGenerator);
		}

		private class DefaultWatermarkGenerator implements WatermarkGenerator<RowData> {
			private final org.apache.flink.table.runtime.generated.WatermarkGenerator innerWatermarkGenerator;
			private Long currentWatermark = Long.MIN_VALUE;

			public DefaultWatermarkGenerator(org.apache.flink.table.runtime.generated.WatermarkGenerator watermarkGenerator) {
				this.innerWatermarkGenerator = watermarkGenerator;
			}

			@Override
			public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
				try {
					currentWatermark = innerWatermarkGenerator.currentWatermark(event);
				} catch (Exception e) {
					throw new RuntimeException(String.format("Generated WatermarkGenerator fails to generate for row: %s.", event), e);
				}
			}

			@Override
			public void onPeriodicEmit(WatermarkOutput output) {
				output.emitWatermark(new Watermark(currentWatermark));
			}
		}

		private class FunctionContextWrapper implements FunctionContext {
			private final Context context;
			public FunctionContextWrapper(Context context) {
				this.context = context;
			}

			@Override
			public MetricGroup getMetricGroup() {
				return context.getMetricGroup();
			}

			@Override
			public File getCachedFile(String name) {
				throw new UnsupportedOperationException("Currently WatermarkGeneratorSupplier.Context doesn't support getCachedFile.");
			}

			@Override
			public String getJobParameter(String key, String defaultValue) {
				throw new UnsupportedOperationException("Currently WatermarkGeneratorSupplier.Context doesn't support getJobParameter.");
			}

			@Override
			public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
				throw new UnsupportedOperationException("Currently WatermarkGeneratorSupplier.Context doesn't support getExternalResourceInfos.");
			}
		}
	}
}
