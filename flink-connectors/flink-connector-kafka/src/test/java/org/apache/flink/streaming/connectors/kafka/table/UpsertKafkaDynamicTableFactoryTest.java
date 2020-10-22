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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link UpsertKafkaDynamicTableFactory}.
 */
public class UpsertKafkaDynamicTableFactoryTest extends TestLogger {

	private static final String SOURCE_TOPICS = "sourceTopic_1;sourceTopic_2";

	private static final List<String> SOURCE_TOPIC_LIST =
			Arrays.asList("sourceTopic_1", "sourceTopic_2");

	private static final String SINK_TOPIC = "sinkTopic";

	private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
			.field("window_start", new AtomicDataType(new VarCharType(false, 100)))
			.field("region", new AtomicDataType(new VarCharType(false, 100)))
			.field("view_count", DataTypes.BIGINT())
			.primaryKey("window_start", "region")
			.build();

	private static final int[] SOURCE_KEY_FIELDS = new int[]{0, 1};

	private static final int[] SOURCE_VALUE_FIELDS = new int[]{0, 1, 2};

	private static final TableSchema SINK_SCHEMA = TableSchema.builder()
			.field("region", new AtomicDataType(new VarCharType(false, 100)))
			.field("view_count", DataTypes.BIGINT())
			.primaryKey("region")
			.build();

	private static final int[] SINK_KEY_FIELDS = new int[]{0};

	private static final int[] SINK_VALUE_FIELDS = new int[]{0, 1};

	private static final Properties UPSERT_KAFKA_SOURCE_PROPERTIES = new Properties();
	private static final Properties UPSERT_KAFKA_SINK_PROPERTIES = new Properties();

	static {
		UPSERT_KAFKA_SOURCE_PROPERTIES.setProperty("bootstrap.servers", "dummy");

		UPSERT_KAFKA_SINK_PROPERTIES.setProperty("bootstrap.servers", "dummy");
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testTableSource() {
		final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

		DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
				new TestFormatFactory.DecodingFormatMock(
						",", true, ChangelogMode.insertOnly());

		DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
				new TestFormatFactory.DecodingFormatMock(
						",", true, ChangelogMode.insertOnly());

		// Construct table source using options and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sourceTable");
		CatalogTable catalogTable =
				new CatalogTableImpl(SOURCE_SCHEMA, getFullSourceOptions(), "sourceTable");
		final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);

		final UpsertKafkaDynamicSource expectedSource = new UpsertKafkaDynamicSource(
				producedDataType,
				SOURCE_TOPIC_LIST,
				null,
				UPSERT_KAFKA_SOURCE_PROPERTIES,
				SOURCE_KEY_FIELDS,
				SOURCE_VALUE_FIELDS,
				keyDecodingFormat,
				valueDecodingFormat);
		assertEquals(actualSource, expectedSource);

		final UpsertKafkaDynamicSource actualUpsertKafkaSource = (UpsertKafkaDynamicSource) actualSource;
		ScanTableSource.ScanRuntimeProvider provider =
				actualUpsertKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(provider, instanceOf(SourceFunctionProvider.class));
		final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
		final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
		assertThat(sourceFunction, instanceOf(FlinkKafkaConsumer.class));
	}

	@Test
	public void testTableSink() {
		final DataType consumedDataType = SINK_SCHEMA.toPhysicalRowDataType();
		EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
				new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());

		EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
				new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());

		// Construct table sink using options and table sink factory.
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sinkTable");
		final CatalogTable sinkTable =
				new CatalogTableImpl(SINK_SCHEMA, getFullSinkOptions(), "sinkTable");
		final DynamicTableSink actualSink = FactoryUtil.createTableSink(
				null,
				objectIdentifier,
				sinkTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);

		final DynamicTableSink expectedSink = new UpsertKafkaDynamicSink(
				consumedDataType,
				SINK_TOPIC,
				UPSERT_KAFKA_SINK_PROPERTIES,
				SINK_KEY_FIELDS,
				SINK_VALUE_FIELDS,
				keyEncodingFormat,
				valueEncodingFormat);

		// Test sink format.
		final UpsertKafkaDynamicSink actualUpsertKafkaSink = (UpsertKafkaDynamicSink) actualSink;
		assertEquals(valueEncodingFormat, actualUpsertKafkaSink.valueEncodingFormat);
		assertEquals(keyEncodingFormat, actualUpsertKafkaSink.keyEncodingFormat);
		assertEquals(expectedSink, actualSink);

		// Test kafka producer.
		DynamicTableSink.SinkRuntimeProvider provider =
				actualUpsertKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
		assertThat(provider, instanceOf(SinkFunctionProvider.class));
		final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
		final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
		assertThat(sinkFunction, instanceOf(FlinkKafkaProducer.class));
	}

	// --------------------------------------------------------------------------------------------
	// Negative tests
	// --------------------------------------------------------------------------------------------

	@Test
	public void testCreateSourceTableWithoutPK() {
		TableSchema illegalSchema = TableSchema.builder()
				.field("window_start", DataTypes.STRING())
				.field("region", DataTypes.STRING())
				.field("view_count", DataTypes.BIGINT())
				.build();
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sourceTable");
		CatalogTable catalogTable = new CatalogTableImpl(illegalSchema, getFullSourceOptions(), "sourceTable");

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("PK constraints must be defined on 'upsert-kafka' connector.")));

		FactoryUtil.createTableSource(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	@Test
	public void testCreateSinkTableWithoutPK() {
		TableSchema illegalSchema = TableSchema.builder()
				.field("region", DataTypes.STRING())
				.field("view_count", DataTypes.BIGINT())
				.build();
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sinkTable");
		CatalogTable catalogTable = new CatalogTableImpl(illegalSchema, getFullSinkOptions(), "sinkTable");

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("PK constraints must be defined on 'upsert-kafka' connector.")));

		FactoryUtil.createTableSink(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	@Test
	public void testSerWithCDCFormat() {
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sinkTable");
		CatalogTable catalogTable = new CatalogTableImpl(
				SINK_SCHEMA,
				getModifiedOptions(
						getFullSinkOptions(),
						options -> options.put(
								String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
								"I;UA;UB;D")),
				"sinkTable");

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(
				new ValidationException("Currently 'upsert-kafka' connector as sink only supports insert-only encoding format.")));

		FactoryUtil.createTableSink(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	@Test
	public void testDeserWithCDCFormat() {
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sourceTable");
		CatalogTable catalogTable = new CatalogTableImpl(
				SOURCE_SCHEMA,
				getModifiedOptions(
						getFullSinkOptions(),
						options -> options.put(
								String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
								"I;UA;UB;D")),
				"sourceTable");

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(
				new ValidationException("Currently 'upsert-kafka' connector as source only supports insert-only decoding format.")));

		FactoryUtil.createTableSource(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the full options modified by the given consumer {@code optionModifier}.
	 *
	 * @param optionModifier Consumer to modify the options
	 */
	private static Map<String, String> getModifiedOptions(
			Map<String, String> options,
			Consumer<Map<String, String>> optionModifier) {
		optionModifier.accept(options);
		return options;
	}

	private static Map<String, String> getFullSourceOptions() {
		// table options
		Map<String, String> options = new HashMap<>();
		options.put("connector", UpsertKafkaDynamicTableFactory.IDENTIFIER);
		options.put("topic", SOURCE_TOPICS);
		options.put("properties.bootstrap.servers", "dummy");
		// format options
		options.put("key.format", TestFormatFactory.IDENTIFIER);
		options.put("value.format", TestFormatFactory.IDENTIFIER);
		options.put(
				String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
		options.put(
				String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key()), "true");
		options.put(
				String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I");
		return options;
	}

	private static Map<String, String> getFullSinkOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", UpsertKafkaDynamicTableFactory.IDENTIFIER);
		options.put("topic", SINK_TOPIC);
		options.put("properties.bootstrap.servers", "dummy");
		// format options
		options.put("value.format", TestFormatFactory.IDENTIFIER);
		options.put("key.format", TestFormatFactory.IDENTIFIER);
		options.put(
				String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
		options.put(
				String.format("%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I"
		);
		return options;
	}
}
