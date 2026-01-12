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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.objectref.ObjectDescriptor;
import org.apache.flink.types.objectref.ObjectRefData;

import java.io.IOException;

public class ObjectRefDataSerializer extends TypeSerializer<ObjectRefData> {

    private static final long serialVersionUID = 1L;

    public static final ObjectRefDataSerializer INSTANCE = new ObjectRefDataSerializer();


    public ObjectRefDataSerializer() {
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ObjectRefData> duplicate() {
        return new ObjectRefDataSerializer();
    }

    @Override
    public ObjectRefData createInstance() {
        return ObjectRefData.createEmptyInstance();
    }

    @Override
    public ObjectRefData copy(ObjectRefData from) {
        return new ObjectRefData(from.toDescriptor());
    }

    @Override
    public ObjectRefData copy(ObjectRefData from, ObjectRefData reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ObjectRefData record, DataOutputView target) throws IOException {
        ObjectDescriptor descriptor = record.toDescriptor();
        StringSerializer.INSTANCE.serialize(descriptor.getURI(), target);
        LongSerializer.INSTANCE.serialize(descriptor.getOffset(), target);
        LongSerializer.INSTANCE.serialize(descriptor.getLength(), target);
    }

    @Override
    public ObjectRefData deserialize(DataInputView source) throws IOException {
        String uri = StringSerializer.INSTANCE.deserialize(source);
        long offset = LongSerializer.INSTANCE.deserialize(source);
        long length = LongSerializer.INSTANCE.deserialize(source);
        return new ObjectRefData(new ObjectDescriptor(uri, offset, length));
    }

    @Override
    public ObjectRefData deserialize(ObjectRefData reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        StringSerializer.INSTANCE.copy(source, target);
        LongSerializer.INSTANCE.copy(source, target);
        LongSerializer.INSTANCE.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        // TODO: rethink it here
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<ObjectRefData> snapshotConfiguration() {
        return new ObjectRefDataSerializerSnapshot();
    }

    @Internal
    public static final class ObjectRefDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ObjectRefData> {

        public ObjectRefDataSerializerSnapshot() {
            super(ObjectRefDataSerializer::new);
        }
    }
}
