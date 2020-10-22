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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * A version-agnostic upsert kafka {@link DynamicTableSink}..
 */
public class UpsertKafkaDynamicSink implements DynamicTableSink, SupportsWritingMetadata {
	// --------------------------------------------------------------------------------------------
	// Mutable attributes
	// --------------------------------------------------------------------------------------------

	/** Data type that describes the final input of the sink. */
	protected DataType consumedDataType;

	/** Metadata that is appended at the end of a physical sink row. */
	protected List<String> metadataKeys;

	// --------------------------------------------------------------------------------------------
	// Format attributes
	// --------------------------------------------------------------------------------------------

	/** Mapping between row data to key part of the records in Kafka. */
	protected final int[] keyFields;

	/** Mapping between row data to value part of the records in Kafka. */
	protected final int[] valueFields;

	/** Sink format for encoding value part of records to Kafka. */
	protected final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;

	/** Sink format for encoding value part of records to Kafka. */
	protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

	/** Data type to configure the format. */
	protected final DataType physicalDataType;

	// --------------------------------------------------------------------------------------------
	// Kafka-specific attributes
	// --------------------------------------------------------------------------------------------

	/** The Kafka topic to write to. */
	protected final String topic;

	/** Properties for the Kafka producer. */
	protected final Properties properties;

	public UpsertKafkaDynamicSink(
			DataType physicalDataType,
			String topic,
			Properties properties,
			int[] keyFields,
			int[] valueFields,
			EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
			EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat) {
		this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
		this.consumedDataType = physicalDataType;
		this.metadataKeys = Collections.emptyList();
		this.topic = Preconditions.checkNotNull(topic, "Topic must not be null.");
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.keyFields = keyFields;
		this.valueFields = valueFields;
		this.keyEncodingFormat = Preconditions.checkNotNull(keyEncodingFormat, "Key encoding format must not be null.");
		this.valueEncodingFormat = Preconditions.checkNotNull(valueEncodingFormat, "Value encoding format must not be null.");
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return ChangelogMode.newBuilder()
				.addContainedKind(RowKind.DELETE)
				.addContainedKind(RowKind.UPDATE_AFTER)
				.build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		final SerializationSchema<RowData> valueSerialization =
				valueEncodingFormat.createRuntimeEncoder(context, this.physicalDataType);
		final SerializationSchema<RowData> keySerialization =
				keyEncodingFormat.createRuntimeEncoder(context,
						DataTypeUtils.projectRow(this.physicalDataType,
								Arrays.stream(keyFields).mapToObj(i -> new int[]{i}).toArray(int[][]::new)));

		final FlinkKafkaProducer<RowData> kafkaProducer =
				createKafkaProducer(keySerialization, valueSerialization);

		return SinkFunctionProvider.of(kafkaProducer);
	}

	@Override
	public Map<String, DataType> listWritableMetadata() {
		final Map<String, DataType> metadataMap = new LinkedHashMap<>();
		Stream.of(RowDataToMetadataConverter.KafkaWritableMetadata.values()).forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
		return metadataMap;
	}

	@Override
	public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
		this.consumedDataType = consumedDataType;
		this.metadataKeys = metadataKeys;
	}

	@Override
	public DynamicTableSink copy() {
		final UpsertKafkaDynamicSink copy = new UpsertKafkaDynamicSink(
				this.physicalDataType,
				this.topic,
				this.properties,
				this.keyFields,
				this.valueFields,
				this.keyEncodingFormat,
				this.valueEncodingFormat);
		copy.consumedDataType = consumedDataType;
		copy.metadataKeys = metadataKeys;
		return copy;
	}

	@Override
	public String asSummaryString() {
		return "upsert kafka table sink";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final UpsertKafkaDynamicSink that = (UpsertKafkaDynamicSink) o;
		return Objects.equals(consumedDataType, that.consumedDataType) &&
				Objects.equals(metadataKeys, that.metadataKeys) &&
				Objects.equals(physicalDataType, that.physicalDataType) &&
				Objects.equals(topic, that.topic) &&
				Objects.equals(properties, that.properties) &&
				Arrays.equals(keyFields, that.keyFields) &&
				Arrays.equals(valueFields, that.valueFields) &&
				Objects.equals(keyEncodingFormat, that.keyEncodingFormat) &&
				Objects.equals(valueEncodingFormat, that.valueEncodingFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				consumedDataType,
				metadataKeys,
				physicalDataType,
				topic,
				properties,
				keyEncodingFormat,
				valueEncodingFormat);
	}

	// --------------------------------------------------------------------------------------------

	protected FlinkKafkaProducer<RowData> createKafkaProducer(
			SerializationSchema<RowData> keySerialization,
			SerializationSchema<RowData> valueSerialization) {
		final List<LogicalType> physicalChildren = physicalDataType.getLogicalType().getChildren();

		final RowData.FieldGetter[] physicalValueFieldGetters = Arrays.stream(valueFields)
				.mapToObj(pos -> RowData.createFieldGetter(physicalChildren.get(pos), pos))
				.toArray(RowData.FieldGetter[]::new);

		final RowData.FieldGetter[] physicalKeyFieldGetters = Arrays.stream(keyFields)
				.mapToObj(pos -> RowData.createFieldGetter(physicalChildren.get(pos), pos))
				.toArray(RowData.FieldGetter[]::new);

		// determine the positions of metadata in the consumed row
		final int[] metadataPositions = Stream.of(RowDataToMetadataConverter.KafkaWritableMetadata.values())
				.mapToInt(m -> {
					final int pos = metadataKeys.indexOf(m.key);
					if (pos < 0) {
						return -1;
					}
					return physicalChildren.size() + pos;
				})
				.toArray();

		// use {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
		// it will use hash partition if key is set else in round-robin behaviour.
		final MetadataKafkaSerializationSchema kafkaSerializer = new MetadataKafkaSerializationSchema(
				topic,
				null,
				keySerialization,
				valueSerialization,
				metadataPositions,
				physicalKeyFieldGetters,
				physicalValueFieldGetters);

		return new FlinkKafkaProducer<>(
				topic,
				kafkaSerializer,
				properties,
				FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
				FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
	}
}
