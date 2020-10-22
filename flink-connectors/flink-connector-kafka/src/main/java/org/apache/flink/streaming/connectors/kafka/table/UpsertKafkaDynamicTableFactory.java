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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPERTIES_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaOptions.TOPIC;

/**
 * Upsert-Kafka factory.
 */
public class UpsertKafkaDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final String IDENTIFIER = "upsert-kafka";

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();
		DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.KEY_FORMAT);
		DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.VALUE_FORMAT);

		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		TableSchema schema = context.getCatalogTable().getSchema();
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(schema);
		List<String> physicalColumns = Arrays.asList(physicalSchema.getFieldNames());
		validateTableSourceOptions(
				tableOptions,
				keyDecodingFormat,
				valueDecodingFormat,
				schema);
		int[] keyFields = schema.getPrimaryKey().get().getColumns().stream()
				.filter(name -> physicalSchema.getTableColumn(name).isPresent())
				.mapToInt(physicalColumns::indexOf)
				.toArray();
		int[] valueFields = IntStream.range(0, physicalSchema.getFieldCount()).toArray();

		return new UpsertKafkaDynamicSource(
				schema.toPhysicalRowDataType(),
				KafkaOptions.getSourceTopics(tableOptions),
				KafkaOptions.getSourceTopicPattern(tableOptions),
				getBootstrapServers(tableOptions),
				keyFields,
				valueFields,
				keyDecodingFormat,
				valueDecodingFormat);
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig tableOptions = helper.getOptions();

		EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat = helper.discoverEncodingFormat(
				SerializationFormatFactory.class,
				FactoryUtil.KEY_FORMAT);
		EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat = helper.discoverEncodingFormat(
				SerializationFormatFactory.class,
				FactoryUtil.VALUE_FORMAT);

		// Validate the option data type.
		helper.validateExcept(KafkaOptions.PROPERTIES_PREFIX);
		TableSchema schema = context.getCatalogTable().getSchema();
		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(schema);
		List<String> physicalColumns = Arrays.asList(physicalSchema.getFieldNames());
		validateTableSinkOptions(
				tableOptions,
				keyEncodingFormat,
				valueEncodingFormat,
				schema);
		int[] keyFields = schema.getPrimaryKey().get().getColumns().stream()
				.filter(name -> physicalSchema.getTableColumn(name).isPresent())
				.mapToInt(physicalColumns::indexOf)
				.toArray();
		int[] valueFields = IntStream.range(0, physicalSchema.getFieldCount()).toArray();

		return new UpsertKafkaDynamicSink(
				schema.toPhysicalRowDataType(),
				tableOptions.get(TOPIC).get(0),
				getBootstrapServers(tableOptions),
				keyFields,
				valueFields,
				keyEncodingFormat,
				valueEncodingFormat);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(PROPS_BOOTSTRAP_SERVERS);
		options.add(FactoryUtil.KEY_FORMAT);
		options.add(FactoryUtil.VALUE_FORMAT);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(KafkaOptions.TOPIC);
		options.add(KafkaOptions.TOPIC_PATTERN);
		options.add(KafkaOptions.SCAN_TOPIC_PARTITION_DISCOVERY);
		return options;
	}

	// --------------------------------------------------------------------------------------------
	// Validation
	// --------------------------------------------------------------------------------------------

	public static void validateTableSourceOptions(
			ReadableConfig tableOptions,
			DecodingFormat<DeserializationSchema<RowData>> keyFormat,
			DecodingFormat<DeserializationSchema<RowData>> valueFormat,
			TableSchema schema) {
		KafkaOptions.validateSourceTopic(tableOptions);
		if (keyFormat.getChangelogMode().contains(RowKind.UPDATE_AFTER) ||
				valueFormat.getChangelogMode().contains(RowKind.UPDATE_AFTER)) {
			throw new ValidationException(
					"Currently 'upsert-kafka' connector as source only supports insert-only decoding format.");
		}
		if (!schema.getPrimaryKey().isPresent()) {
			throw new ValidationException("PK constraints must be defined on 'upsert-kafka' connector.");
		}
	}

	public static void validateTableSinkOptions(
			ReadableConfig tableOptions,
			EncodingFormat<SerializationSchema<RowData>> keyFormat,
			EncodingFormat<SerializationSchema<RowData>> valueFormat,
			TableSchema schema) {
		KafkaOptions.validateSinkTopic(tableOptions);
		if (keyFormat.getChangelogMode().contains(RowKind.UPDATE_AFTER) ||
				valueFormat.getChangelogMode().contains(RowKind.UPDATE_AFTER)) {
			throw new ValidationException(
					"Currently 'upsert-kafka' connector as sink only supports insert-only encoding format.");
		}
		if (!schema.getPrimaryKey().isPresent()) {
			throw new ValidationException("PK constraints must be defined on 'upsert-kafka' connector.");
		}
	}

	// --------------------------------------------------------------------------------------------
	// Utils
	// --------------------------------------------------------------------------------------------

	public static Properties getBootstrapServers(ReadableConfig tableOptions) {
		String key = PROPS_BOOTSTRAP_SERVERS.key().substring(PROPERTIES_PREFIX.length());
		String value = tableOptions.get(PROPS_BOOTSTRAP_SERVERS);
		Properties props = new Properties();
		props.put(key, value);
		return props;
	}
}
