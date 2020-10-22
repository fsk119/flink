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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Metadata ennum.
 */
interface MetadataToRowDataConverter extends Serializable {
	enum KafkaReadableMetadata {
		TOPIC(
				"topic",
				DataTypes.STRING().notNull(),
				record -> StringData.fromString(record.topic())
		),

		PARTITION(
				"partition",
				DataTypes.INT().notNull(),
				ConsumerRecord::partition
		),

		HEADERS(
				"headers",
				// key and value of the map are nullable to make handling easier in queries
				DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable()).notNull(),
				record -> {
					final Map<StringData, byte[]> map = new HashMap<>();
					for (Header header : record.headers()) {
						map.put(StringData.fromString(header.key()), header.value());
					}
					return new GenericMapData(map);
				}
		),

		LEADER_EPOCH(
				"leader-epoch",
				DataTypes.INT().nullable(),
				record -> record.leaderEpoch().orElse(null)
		),

		OFFSET(
				"offset",
				DataTypes.BIGINT().notNull(),
				ConsumerRecord::offset),

		TIMESTAMP(
				"timestamp",
				DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
				record -> TimestampData.fromEpochMillis(record.timestamp())),

		TIMESTAMP_TYPE(
				"timestamp-type",
				DataTypes.STRING().notNull(),
				record -> StringData.fromString(record.timestampType().toString())
		);

		final String key;

		final DataType dataType;

		final MetadataToRowDataConverter converter;

		KafkaReadableMetadata(String key, DataType dataType, MetadataToRowDataConverter converter) {
			this.key = key;
			this.dataType = dataType;
			this.converter = converter;
		}
	}

	Object read(ConsumerRecord<?, ?> record);
}

