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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.List;

/** Info represents a {@link RowData}. */
@Internal
public class RowDataInfo {

    protected static final String NULL_VALUE = "null";

    private final RowKind rowKind;
    private final List<Object> fields;

    public RowDataInfo(RowKind rowKind, List<Object> fields) {
        this.rowKind = Preconditions.checkNotNull(rowKind, "row kind must not be null.");
        this.fields = Preconditions.checkNotNull(fields, "fields must not be null.");
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public List<Object> getFields() {
        return fields;
    }

    public RowData toRowData() {
        return GenericRowData.ofKind(rowKind, fields.toArray());
    }

    public String[] toStringifiedFields() {
        return fields.stream()
                .map(field -> field == null ? NULL_VALUE : field.toString())
                .toArray(String[]::new);
    }
}
