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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryObjectRefData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.types.objectref.ObjectRef;
import org.apache.flink.types.objectref.ObjectRefData;

import java.io.IOException;

public class ObjectRefSerializer extends TypeSerializer<ObjectRef> {


    public ObjectRefSerializer() {
    }


    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ObjectRef> duplicate() {
        return new ObjectRefSerializer();
    }

    @Override
    public ObjectRef createInstance() {
        return ObjectRefData.createEmptyInstance();
    }

    @Override
    public ObjectRef copy(ObjectRef from) {
        BinaryObjectRefData binaryObjectRefData;
        if (from instanceof ObjectRefData) {
            binaryObjectRefData = new BinaryObjectRefData((ObjectRefData) from);
        } else if (from instanceof BinaryObjectRefData) {
            binaryObjectRefData = (BinaryObjectRefData) from;
        } else {
            throw new UnsupportedOperationException();
        }

        binaryObjectRefData.ensureMaterialized(null);
        byte[] copy =
                BinarySegmentUtils.copyToBytes(
                        binaryObjectRefData.getSegments(),
                        binaryObjectRefData.getOffset(),
                        binaryObjectRefData.getSizeInBytes());
        return new BinaryObjectRefData(
                new MemorySegment[] {MemorySegmentFactory.wrap(copy)},
                0,
                binaryObjectRefData.getSizeInBytes());
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
        BinaryObjectRefData binaryObjectRefData;
        if (record instanceof ObjectRefData) {
            binaryObjectRefData = new BinaryObjectRefData((ObjectRefData) record);
        } else if (record instanceof BinaryObjectRefData) {
            binaryObjectRefData = (BinaryObjectRefData) record;
        } else {
            throw new UnsupportedOperationException();
        }

        binaryObjectRefData.ensureMaterialized(null);
        target.writeInt(binaryObjectRefData.getSizeInBytes());
        BinarySegmentUtils.copyToView(
                binaryObjectRefData.getSegments(),
                binaryObjectRefData.getOffset(),
                binaryObjectRefData.getSizeInBytes(),
                target);
    }

    @Override
    public ObjectRef deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return new BinaryObjectRefData(
                new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, bytes.length);
    }

    @Override
    public ObjectRef deserialize(ObjectRef reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    @Override
    public boolean equals(Object obj) {
        // TODO: rethink here.
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<ObjectRef> snapshotConfiguration() {
        return new ObjectRefSerializerSnapshot();
    }

    @Internal
    public static final class ObjectRefSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ObjectRef> {
        // TODO: it's right?

        public ObjectRefSerializerSnapshot() {
            super(ObjectRefSerializer::new);
        }
    }
}
