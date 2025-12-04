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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryObjectRefData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.types.objectref.ByteArrayAccessor;
import org.apache.flink.types.objectref.FileAccessor;
import org.apache.flink.types.objectref.ObjectAccessor;
import org.apache.flink.types.objectref.ObjectRef;
import org.apache.flink.types.objectref.ObjectRefData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ObjectRefDataSerializer extends TypeSerializerSingleton<ObjectRef> {

    private static final ObjectRefDataSerializer INSTANCE = new ObjectRefDataSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public ObjectRef createInstance() {
        return ObjectRefData.createEmptyInstance();
    }

    @Override
    public ObjectRef copy(ObjectRef from) {
        if (from instanceof ObjectRefData) {
            return org.apache.flink.api.common.typeutils.base.ObjectRefDataSerializer.INSTANCE.copy(
                    (ObjectRefData) from);
        } else if (from instanceof BinaryObjectRefData) {
            BinaryObjectRefData binaryObjectRefData = (BinaryObjectRefData) from;
            byte[] copy =
                    BinarySegmentUtils.copyToBytes(
                            binaryObjectRefData.getSegments(),
                            binaryObjectRefData.getOffset(),
                            binaryObjectRefData.getSizeInBytes());
            BinaryObjectRefData copied = new BinaryObjectRefData();
            copied.pointTo(
                    MemorySegmentFactory.wrap(copy), 0, binaryObjectRefData.getSizeInBytes());
            return copied;
        } else {
            throw new UnsupportedOperationException();
        }
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
        if (record instanceof ObjectRefData) {
            byte[] binaryString = StringData.fromString(record.getContentType()).toBytes();
            // add assert here
            int len = binaryString.length;
            ObjectAccessor accessor = record.getAccessor();
            int type;
            if (accessor instanceof ByteArrayAccessor) {
                type = 0;
            } else if (accessor instanceof FileAccessor) {
                type = 1;
            } else {
                type = 2;
            }

            int lenAndType = (len << 8) | type;

            MemorySegment memorySegment = MemorySegmentFactory.wrapInt(lenAndType);
            memorySegment.putInt(0, lenAndType);
        }
    }

    @Override
    public ObjectRef deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public ObjectRef deserialize(ObjectRef reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {}

    @Override
    public TypeSerializerSnapshot<ObjectRef> snapshotConfiguration() {
        return null;
    }
}
