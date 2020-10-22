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
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * TO be continued.
 */
public class MetadataKafkaSerializationSchema implements KafkaSerializationSchema<RowData>, KafkaContextAware<RowData> {

	private final @Nullable
	FlinkKafkaPartitioner<RowData> partitioner;

	private final String topic;

	private final SerializationSchema<RowData> keySerialization;

	private final SerializationSchema<RowData> valueSerialization;

	/**
	 * Contains the position for each value of {@link RowDataToMetadataConverter.KafkaWritableMetadata} in the consumed row or
	 * -1 if this metadata key is not used.
	 */
	private final int[] metadataPositions;

	private final RowData.FieldGetter[] keyFieldGetters;

	private final RowData.FieldGetter[] valueFieldGetters;

	private int[] partitions;

	private int parallelInstanceId;

	private int numParallelInstances;

	MetadataKafkaSerializationSchema(
			String topic,
			@Nullable FlinkKafkaPartitioner<RowData> partitioner,
			SerializationSchema<RowData> keySerialization,
			SerializationSchema<RowData> valueSerialization,
			int[] metadataPositions,
			RowData.FieldGetter[] keyFieldGetters,
			RowData.FieldGetter[] valueFieldGetters) {
		this.topic = topic;
		this.partitioner = partitioner;
		this.keySerialization = keySerialization;
		this.valueSerialization = valueSerialization;
		this.metadataPositions = metadataPositions;
		this.keyFieldGetters = keyFieldGetters;
		this.valueFieldGetters = valueFieldGetters;
	}

	@Override
	public void open(SerializationSchema.InitializationContext context) throws Exception {
		keySerialization.open(context);
		valueSerialization.open(context);
		if (partitioner != null) {
			partitioner.open(parallelInstanceId, numParallelInstances);
		}
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(RowData consumedRow, @Nullable Long timestamp) {
		final int physicalArity = valueFieldGetters.length;
		final boolean hasMetadata = physicalArity != consumedRow.getArity();

		final GenericRowData keyPhysicalRow, valuePhysicalRow;
		keyPhysicalRow = new GenericRowData(
				consumedRow.getRowKind(),
				keyFieldGetters.length);
		for (int i = 0; i < keyFieldGetters.length; i++) {
			keyPhysicalRow.setField(i, keyFieldGetters[i].getFieldOrNull(consumedRow));
		}

		if (consumedRow.getRowKind().equals(RowKind.DELETE) ||
				consumedRow.getRowKind().equals(RowKind.UPDATE_BEFORE)) {
			valuePhysicalRow = null;
		} else {
			valuePhysicalRow = new GenericRowData(
					consumedRow.getRowKind(),
					valueFieldGetters.length);
			for (int i = 0; i < valueFieldGetters.length; i++) {
				valuePhysicalRow.setField(i, valueFieldGetters[i].getFieldOrNull(consumedRow));
			}
		}

		final byte[] keySerialized = keySerialization.serialize(keyPhysicalRow);

		final byte[] valueSerialized = valueSerialization.serialize(valuePhysicalRow);

		final Integer partition;
		if (partitioner != null) {
			partition = partitioner.partition(keyPhysicalRow, keySerialized, valueSerialized, topic, partitions);
		} else {
			partition = null;
		}

		// shortcut if no metadata is required
		if (!hasMetadata) {
			return new ProducerRecord<>(topic, partition, null, keySerialized, valueSerialized);
		}
		return new ProducerRecord<>(
				topic,
				partition,
				readMetadata(consumedRow, RowDataToMetadataConverter.KafkaWritableMetadata.TIMESTAMP),
				keySerialized,
				valueSerialized,
				readMetadata(consumedRow, RowDataToMetadataConverter.KafkaWritableMetadata.HEADERS));
	}

	@Override
	public void setParallelInstanceId(int parallelInstanceId) {
		this.parallelInstanceId = parallelInstanceId;
	}

	@Override
	public void setNumParallelInstances(int numParallelInstances) {
		this.numParallelInstances = numParallelInstances;
	}

	@Override
	public void setPartitions(int[] partitions) {
		this.partitions = partitions;
	}

	@Override
	public String getTargetTopic(RowData element) {
		return topic;
	}

	@SuppressWarnings("unchecked")
	private <T> T readMetadata(RowData consumedRow, RowDataToMetadataConverter.KafkaWritableMetadata metadata) {
		final int pos = metadataPositions[metadata.ordinal()];
		if (pos < 0) {
			return null;
		}
		return (T) metadata.converter.read(consumedRow, pos);
	}
}
