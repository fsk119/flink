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

import org.apache.flink.api.common.typeutils.TypeSerializer;;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.types.objectref.ByteArrayAccessorSerializer;
import org.apache.flink.types.objectref.FileAccessorSerializer;
import org.apache.flink.types.objectref.ObjectAccessor;
import org.apache.flink.types.objectref.ObjectRef;

import java.io.ByteArrayInputStream;

// It depends on the accessor type:
// * type = 0, it means byte array. | metadata(3 byte len + 1 byte type) | content | bytearray |
// * type = 1, it means string.     | metadata(3 byte len + 1 byte type) | content | file address |
// * type = 2, it means user-defined accessor.
// | metadata(3 byte len + 1 byte type) | content | snapshot len | snapshot | data |
public class BinaryObjectRefData extends BinarySection implements ObjectRef {

    private BinaryStringData content;
    private RawValueData<ObjectAccessor> accessor;
    private TypeSerializer<? extends ObjectAccessor> serializer;

    @Override
    public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
        super.pointTo(segments, offset, sizeInBytes);
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
                serializer =
                        (TypeSerializer)
                                TypeSerializerSnapshotSerializationUtil
                                        .readAndInstantiateSnapshotClass(
                                                new DataInputViewStreamWrapper(inputStream),
                                                Thread.currentThread().getContextClassLoader())
                                        .restoreSerializer();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            accessor =
                    new BinaryRawValueData<>(
                            segments,
                            offset + 4 + content.getOffset() + 4 + snapshotLen,
                            sizeInBytes - 4 - content.getSizeInBytes() - 4 - snapshotLen);
        }
    }

    @Override
    public ObjectAccessor getAccessor() {
        return accessor.toObject((TypeSerializer) serializer);
    }

    @Override
    public String getContentType() {
        assert content != null;
        return content.toString();
    }
}
