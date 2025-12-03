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
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.objectref.ByteArrayAccessor;
import org.apache.flink.types.objectref.ByteArrayAccessorSerializer;
import org.apache.flink.types.objectref.FileAccessor;
import org.apache.flink.types.objectref.FileAccessorSerializer;
import org.apache.flink.types.objectref.ObjectAccessor;
import org.apache.flink.types.objectref.ObjectRefData;
import org.apache.flink.util.InstantiationUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ObjectRefDataSerializer extends TypeSerializerSingleton<ObjectRefData> {

    private static final long serialVersionUID = 1L;

    public static final ObjectRefDataSerializer INSTANCE = new ObjectRefDataSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public ObjectRefData createInstance() {
        return ObjectRefData.createEmptyInstance();
    }

    @Override
    public ObjectRefData copy(ObjectRefData from) {
        return new ObjectRefData(
                from.getContentType(), from.getAccessor().getSerializer().copy(from.getAccessor()));
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
        if (record == null) {
            target.write(0);
            return;
        }
        StringSerializer.INSTANCE.serialize(record.getContentType(), target);
        ObjectAccessor accessor = record.getAccessor();
        TypeSerializer<ObjectAccessor> serializer = accessor.getSerializer();
        if (accessor instanceof ByteArrayAccessor) {
            target.writeByte(0);
            serializer.serialize(accessor, target);
        } else if (accessor instanceof FileAccessor) {
            target.writeByte(1);
            serializer.serialize(accessor, target);
        } else {
            target.writeByte(2);
            try (ByteArrayOutputStream out = new ByteArrayOutputStream(); ) {
                serializer
                        .snapshotConfiguration()
                        .writeSnapshot(new DataOutputViewStreamWrapper(out));
                byte[] snapshotBytes = out.toByteArray();
                byte[] data = InstantiationUtil.serializeToByteArray(serializer, accessor);
                target.writeInt(snapshotBytes.length + data.length);
                target.write(snapshotBytes);
                target.write(data);
            }
        }
    }

    @Override
    public ObjectRefData deserialize(DataInputView source) throws IOException {
        String content = StringSerializer.INSTANCE.deserialize(source);
        if (content == null) {
            return null;
        }

        byte type = source.readByte();
        ObjectAccessor accessor;
        if (type == 0) {
            accessor = ByteArrayAccessorSerializer.INSTANCE.deserialize(source);
        } else if (type == 1) {
            accessor = FileAccessorSerializer.INSTANCE.deserialize(source);
        } else if (type == 2) {
            // ignore size
            source.skipBytesToRead(4);
            accessor =
                    (ObjectAccessor)
                            TypeSerializerSnapshot.readVersionedSnapshot(
                                            source, Thread.currentThread().getContextClassLoader())
                                    .restoreSerializer()
                                    .deserialize(source);
        } else {
            throw new UnsupportedOperationException();
        }

        return new ObjectRefData(content, accessor);
    }

    @Override
    public ObjectRefData deserialize(ObjectRefData reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // TODO: it's not correct
        StringSerializer.INSTANCE.copy(source, target);

        int type = source.readUnsignedByte();
        target.writeByte(type);
        if (type == 0) {
            ByteArrayAccessorSerializer.INSTANCE.copy(source, target);
        } else if (type == 1) {
            FileAccessorSerializer.INSTANCE.copy(source, target);
        } else if (type == 2) {
            int size = source.readUnsignedByte();
            target.write(source, size);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public TypeSerializerSnapshot<ObjectRefData> snapshotConfiguration() {
        return new ObjectRefDataSerializerSnapshot();
    }

    @Internal
    public static final class ObjectRefDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ObjectRefData> {

        public ObjectRefDataSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
