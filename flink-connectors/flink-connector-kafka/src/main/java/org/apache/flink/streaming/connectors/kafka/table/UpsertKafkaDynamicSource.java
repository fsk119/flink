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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A version-agnostic upsert kafka {@link ScanTableSource}.
 */
public class UpsertKafkaDynamicSource implements ScanTableSource, SupportsReadingMetadata {

	// --------------------------------------------------------------------------------------------
	// Mutable attributes
	// --------------------------------------------------------------------------------------------

	/** Data type that describes the final output of the source. */
	protected DataType producedDataType;

	/** Metadata that is appended at the end of a physical source row. */
	protected List<String> metadataKeys;

	// --------------------------------------------------------------------------------------------
	// Format attributes
	// --------------------------------------------------------------------------------------------
	/** Mapping between row data to key part of the records in Kafka. */
	protected final int[] keyFields;

	/** Mapping between row data to value part of the records in Kafka. */
	protected final int[] valueFields;

	/** Scan format for decoding key part of the records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

	/** Scan format for decoding value part of the records from Kafka. */
	protected final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

	/** Data type to configure the format. */
	protected final DataType physicalDataType;

	// --------------------------------------------------------------------------------------------
	// Kafka-specific attributes
	// --------------------------------------------------------------------------------------------

	/** The Kafka topics to consume. */
	protected final List<String> topics;

	/** The Kafka topic pattern to consume. */
	protected final Pattern topicPattern;

	/** Properties for the Kafka consumer. */
	protected final Properties properties;

	public UpsertKafkaDynamicSource(
			DataType physicalDataType,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			int[] keyFields,
			int[] valueFields,
			DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormaat,
			DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat) {
		this.physicalDataType = Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
		this.producedDataType = physicalDataType;
		this.metadataKeys = Collections.emptyList();
		Preconditions.checkArgument((topics != null && topicPattern == null) ||
						(topics == null && topicPattern != null),
				"Either Topic or Topic Pattern must be set for source.");
		this.topics = topics;
		this.topicPattern = topicPattern;
		this.properties = Preconditions.checkNotNull(properties, "Properties must not be null.");
		this.keyFields = keyFields;
		this.valueFields = valueFields;
		this.valueDecodingFormat = Preconditions.checkNotNull(
				keyDecodingFormaat, "Value decoding format must not be null.");
		this.keyDecodingFormat = Preconditions.checkNotNull(
				valueDecodingFormat, "Key decoding format must not be null.");
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.newBuilder()
				.addContainedKind(RowKind.UPDATE_AFTER)
				.addContainedKind(RowKind.DELETE)
				.build();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		final DeserializationSchema<RowData> valueDeserialization =
				valueDecodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalDataType);

		final TypeInformation<RowData> producedTypeInfo =
				runtimeProviderContext.createTypeInformation(producedDataType);

		final FlinkKafkaConsumer<RowData> kafkaConsumer = createKafkaConsumer(valueDeserialization, producedTypeInfo);

		return SourceFunctionProvider.of(kafkaConsumer, false);
	}

	@Override
	public Map<String, DataType> listReadableMetadata() {
		final Map<String, DataType> metadataMap = new LinkedHashMap<>();
		Stream.of(MetadataToRowDataConverter.KafkaReadableMetadata.values())
				.forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
		return metadataMap;
	}

	@Override
	public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
		this.metadataKeys = metadataKeys;
		this.producedDataType = producedDataType;
	}

	@Override
	public DynamicTableSource copy() {
		final UpsertKafkaDynamicSource copy = new UpsertKafkaDynamicSource(
				this.physicalDataType,
				this.topics,
				this.topicPattern,
				this.properties,
				this.keyFields,
				this.valueFields,
				this.keyDecodingFormat,
				this.valueDecodingFormat);
		copy.producedDataType = producedDataType;
		copy.metadataKeys = metadataKeys;
		return copy;
	}

	@Override
	public String asSummaryString() {
		return "upsert kafka table source";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final UpsertKafkaDynamicSource that = (UpsertKafkaDynamicSource) o;
		return Objects.equals(producedDataType, that.producedDataType) &&
				Objects.equals(metadataKeys, that.metadataKeys) &&
				Objects.equals(physicalDataType, that.physicalDataType) &&
				Objects.equals(topics, that.topics) &&
				Objects.equals(String.valueOf(topicPattern), String.valueOf(that.topicPattern)) &&
				Objects.equals(properties, that.properties) &&
				Arrays.equals(keyFields, that.keyFields) &&
				Arrays.equals(valueFields, that.valueFields) &&
				Objects.equals(keyDecodingFormat, that.keyDecodingFormat) &&
				Objects.equals(valueDecodingFormat, that.valueDecodingFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				producedDataType,
				metadataKeys,
				physicalDataType,
				topics,
				topicPattern,
				properties,
				keyDecodingFormat,
				valueDecodingFormat);
	}

	// --------------------------------------------------------------------------------------------

	protected FlinkKafkaConsumer<RowData> createKafkaConsumer(
			DeserializationSchema<RowData> valueDeserialization,
			TypeInformation<RowData> producedTypeInfo) {

		final MetadataToRowDataConverter[] metadataConverters = metadataKeys.stream()
				.map(k ->
						Stream.of(MetadataToRowDataConverter.KafkaReadableMetadata.values())
								.filter(rm -> rm.key.equals(k))
								.findFirst()
								.orElseThrow(IllegalStateException::new))
				.map(m -> m.converter)
				.toArray(MetadataToRowDataConverter[]::new);

		final KafkaDeserializationSchema<RowData> kafkaDeserializer = new MetadataKafkaDeserializationSchema(
				valueDeserialization,
				metadataConverters,
				producedTypeInfo);

		final FlinkKafkaConsumer<RowData> kafkaConsumer;
		if (topics != null) {
			kafkaConsumer = new FlinkKafkaConsumer<>(topics, kafkaDeserializer, properties);
		} else {
			kafkaConsumer = new FlinkKafkaConsumer<>(topicPattern, kafkaDeserializer, properties);
		}
		// always read from the earliest offset
		kafkaConsumer.setStartFromEarliest();
		return kafkaConsumer;
	}
}
