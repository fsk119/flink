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

package org.apache.flink.table.data.binary;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.DynamicTypeRegistry;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.objectref.ByteArrayAccessor;
import org.apache.flink.types.objectref.ByteArrayAccessorSerializer;
import org.apache.flink.types.objectref.FileAccessor;
import org.apache.flink.types.objectref.FileAccessorSerializer;
import org.apache.flink.types.objectref.ObjectAccessor;
import org.apache.flink.types.objectref.ObjectRef;
import org.apache.flink.types.objectref.ObjectRefData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

// It depends on the accessor type:
// * type = 0, it means byte array. | metadata(3 byte len + 1 byte type) | content | bytearray |
// * type = 1, it means string.     | metadata(3 byte len + 1 byte type) | content | file address |
// * type = 2, it means user-defined accessor.
// | metadata(3 byte len + 1 byte type) | content | snapshot len | snapshot | data |
public class BinaryObjectRefData extends LazyBinaryFormat<ObjectRef> implements ObjectRef {

    private BinaryStringData content;
    private RawValueData<ObjectAccessor> accessor;
    private TypeSerializer<? extends ObjectAccessor> serializer;

    public BinaryObjectRefData(ObjectRefData refData) {
        super(refData);
    }

    public BinaryObjectRefData(MemorySegment[] segments, int offset, int sizeInBytes) {
        super(null, new BinarySection(segments, offset, sizeInBytes));
    }

    @Override
    public ObjectAccessor getAccessor() {
        if (javaObject == null) {
            deserialize();
        }
        return javaObject.getAccessor();
    }

    @Override
    public String getContentType() {
        if (javaObject == null) {
            deserialize();
        }
        return javaObject.getContentType();
    }

    private void deserialize() {
        MemorySegment[] segments = binarySection.getSegments();
        int offset = binarySection.getOffset();
        int sizeInBytes = binarySection.getSizeInBytes();
        int lenAndType = BinarySegmentUtils.getInt(segments, offset);
        int serializerType = lenAndType & 0xFF;
        content = BinaryStringData.fromAddress(segments, offset + 4, (lenAndType >>> 8));
        if (serializerType == 0) {
            serializer = ByteArrayAccessorSerializer.INSTANCE;
            accessor =
                    new BinaryRawValueData<>(
                            segments,
                            content.getOffset() + content.getSizeInBytes(),
                            sizeInBytes - 4 - content.getSizeInBytes());
        } else if (serializerType == 1) {
            serializer = FileAccessorSerializer.INSTANCE;
            accessor =
                    new BinaryRawValueData<>(
                            segments,
                            content.getOffset() + content.getSizeInBytes(),
                            sizeInBytes - 4 - content.getSizeInBytes());
        } else if (serializerType == 2) {
            int snapshotLen = BinarySegmentUtils.getInt(segments, offset + 4 + content.getOffset());
            ByteArrayInputStream inputStream =
                    new ByteArrayInputStream(
                            BinarySegmentUtils.copyToBytes(
                                    segments,
                                    content.getOffset() + content.getSizeInBytes(),
                                    snapshotLen));
            try {
                // TODO: here
                // if its type is simple type
                String className = new DataInputViewStreamWrapper(inputStream).readUTF();
                serializer =
                        DynamicTypeRegistry.getInstance()
                                .getSerializer(
                                        Thread.currentThread().getContextClassLoader(), className);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            accessor =
                    new BinaryRawValueData<>(
                            segments,
                            offset + 4 + content.getOffset() + 4 + snapshotLen,
                            sizeInBytes - 4 - content.getSizeInBytes() - 4 - snapshotLen);
        }

        javaObject =
                new ObjectRefData(
                        content.javaObject,
                        accessor.toObject((TypeSerializer<ObjectAccessor>) serializer));
    }

    @Override
    protected BinarySection materialize(TypeSerializer<ObjectRef> serializer) throws IOException {
        byte[] binaryString = StringData.fromString(javaObject.getContentType()).toBytes();
        // add assert here
        int len = binaryString.length;
        ObjectAccessor accessor = javaObject.getAccessor();
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

        try (ByteArrayOutputStream out = new ByteArrayOutputStream(lenAndType + 1)) {
            DataOutputView view = new DataOutputViewStreamWrapper(out);
            view.write(binaryString);
            if (type == 2) {
                javaObject
                        .getAccessor()
                        .getSerializer()
                        .snapshotConfiguration()
                        .writeSnapshot(view);
            }
            javaObject.getAccessor().getSerializer().serialize(javaObject.getAccessor(), view);
            byte[] bytes = out.toByteArray();
            return new BinarySection(
                    new MemorySegment[] {memorySegment, MemorySegmentFactory.wrap(bytes)},
                    0,
                    bytes.length + 4);
        }
    }
}
