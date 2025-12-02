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
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.objectref.ObjectAccessor;
import org.apache.flink.types.objectref.ObjectRef;
import org.apache.flink.types.variant.Variant;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.function.Supplier;

public class ObjectRefSerializer extends TypeSerializerSingleton<ObjectRef> {

    private static final long serialVersionUID = 1L;

    public static final ObjectRefSerializer INSTANCE = new ObjectRefSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public ObjectRef createInstance() {
        return ObjectRef.createEmptyInstance();
    }

    @Override
    public ObjectRef copy(ObjectRef from) {
        return new ObjectRef(from.getContentType(), from.getAccessor().copy());
    }

    @Override
    public ObjectRef copy(ObjectRef from, ObjectRef reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ObjectRef record, DataOutputView target) throws IOException {
        if (record == null) {
            target.write(0);
            return;
        }
        StringSerializer.INSTANCE.serialize(record.getContentType(), target);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(out)) {
            objectOutputStream.writeObject(record.getAccessor());
            byte[] serializedAccessor = out.toByteArray();
            BytePrimitiveArraySerializer.INSTANCE.serialize(serializedAccessor, target);
        }
    }

    @Override
    public ObjectRef deserialize(DataInputView source) throws IOException {
        String content = StringSerializer.INSTANCE.deserialize(source);
        if (content == null) {
            return null;
        } else {
            try (ObjectInputStream objectInputStream =
                    new ObjectInputStream(
                            new ByteArrayInputStream(
                                    BytePrimitiveArraySerializer.INSTANCE.deserialize(source)))) {
                return new ObjectRef(content, (ObjectAccessor) objectInputStream.readObject());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to deserialize the accessor.", e);
            }
        }
    }

    @Override
    public ObjectRef deserialize(ObjectRef reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // TODO: it's not correct
        StringSerializer.INSTANCE.copy(source, target);
        BytePrimitiveArraySerializer.INSTANCE.copy(source, target);
    }

    @Override
    public TypeSerializerSnapshot<ObjectRef> snapshotConfiguration() {
        return new ObjectRefSerializerSnapshot();
    }

    @Internal
    public static final class ObjectRefSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ObjectRef> {

        public ObjectRefSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
