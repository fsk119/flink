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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.kafka.common.header.Header;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Convert RowData to record metadata.
 */
interface RowDataToMetadataConverter extends Serializable {
	enum KafkaWritableMetadata {
		HEADERS(
				"headers",
				// key and value of the map are nullable to make handling easier in queries
				DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable()).nullable(),
				(row, pos) -> {
					if (row.isNullAt(pos)) {
						return null;
					}
					final MapData map = row.getMap(pos);
					final ArrayData keyArray = map.keyArray();
					final ArrayData valueArray = map.valueArray();
					final List<Header> headers = new ArrayList<>();
					for (int i = 0; i < keyArray.size(); i++) {
						if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
							final String key = keyArray.getString(i).toString();
							final byte[] value = valueArray.getBinary(i);
							headers.add(new KafkaHeader(key, value));
						}
					}
					return headers;
				}
		),

		TIMESTAMP(
				"timestamp",
				DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
				(row, pos) -> {
					if (row.isNullAt(pos)) {
						return null;
					}
					return row.getTimestamp(pos, 3).getMillisecond();
				});

		final String key;

		final DataType dataType;

		final RowDataToMetadataConverter converter;

		KafkaWritableMetadata(String key, DataType dataType, RowDataToMetadataConverter converter) {
			this.key = key;
			this.dataType = dataType;
			this.converter = converter;
		}
	}

	Object read(RowData consumedRow, int pos);

	class KafkaHeader implements Header {
		private final String key;

		private final byte[] value;

		KafkaHeader(String key, byte[] value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String key() {
			return key;
		}

		@Override
		public byte[] value() {
			return value;
		}
	}
}

