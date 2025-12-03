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

package org.apache.flink.types.objectref;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class ByteArrayAccessorSerializer extends TypeSerializer<ByteArrayAccessor> {

    public static final ByteArrayAccessorSerializer INSTANCE = new ByteArrayAccessorSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ByteArrayAccessor> duplicate() {
        return INSTANCE;
    }

    @Override
    public ByteArrayAccessor createInstance() {
        return new ByteArrayAccessor(new byte[0]);
    }

    @Override
    public ByteArrayAccessor copy(ByteArrayAccessor from) {
        return new ByteArrayAccessor(from.getBytesInternal());
    }

    @Override
    public ByteArrayAccessor copy(ByteArrayAccessor from, ByteArrayAccessor reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ByteArrayAccessor record, DataOutputView target) throws IOException {
        BytePrimitiveArraySerializer.INSTANCE.serialize(record.getBytesInternal(), target);
    }

    @Override
    public ByteArrayAccessor deserialize(DataInputView source) throws IOException {
        return new ByteArrayAccessor(BytePrimitiveArraySerializer.INSTANCE.deserialize(source));
    }

    @Override
    public ByteArrayAccessor deserialize(ByteArrayAccessor reuse, DataInputView source)
            throws IOException {
        return new ByteArrayAccessor(BytePrimitiveArraySerializer.INSTANCE.deserialize(source));
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        BytePrimitiveArraySerializer.INSTANCE.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ByteArrayAccessorSerializer;
    }

    @Override
    public int hashCode() {
        return ByteArrayAccessorSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<ByteArrayAccessor> snapshotConfiguration() {
        return new ByteArrayAccessorSerializerSnapshot();
    }

    public static class ByteArrayAccessorSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ByteArrayAccessor> {

        public ByteArrayAccessorSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
