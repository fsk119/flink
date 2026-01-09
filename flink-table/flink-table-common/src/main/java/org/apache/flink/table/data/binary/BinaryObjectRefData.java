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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.objectref.ObjectAccessor;
import org.apache.flink.types.objectref.ObjectAccessorRegistry;
import org.apache.flink.types.objectref.ObjectDescriptor;
import org.apache.flink.types.objectref.ObjectRef;
import org.apache.flink.types.objectref.ObjectRefData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class BinaryObjectRefData extends LazyBinaryFormat<ObjectRef> implements ObjectRef {

    private ObjectAccessorRegistry registry;

    public BinaryObjectRefData(ObjectRefData refData) {
        super(refData);
    }

    public BinaryObjectRefData(
            MemorySegment[] segments,
            int offset,
            int sizeInBytes,
            ObjectAccessorRegistry registry) {
        super(null, new BinarySection(segments, offset, sizeInBytes));
        this.registry = registry;
    }

    @Override
    public ObjectDescriptor toDescriptor() {
        if (javaObject == null) {
            deserialize();
        }
        return javaObject.toDescriptor();
    }

    @Override
    public ObjectAccessor getAccessor() {
        if (javaObject == null) {
            deserialize();
        }
        return javaObject.getAccessor();
    }

    private void deserialize() {
        MemorySegment[] segments = binarySection.getSegments();
        String uri =
                BinaryStringData.fromAddress(
                                segments, getOffset(), binarySection.getSizeInBytes() - 16)
                        .javaObject;
        long offset =
                BinarySegmentUtils.getLong(
                        segments, getOffset() + binarySection.getSizeInBytes() - 16);
        long length =
                BinarySegmentUtils.getLong(
                        segments, getOffset() + binarySection.getSizeInBytes() - 8);
        javaObject = new ObjectRefData(new ObjectDescriptor(uri, offset, length), registry);
    }

    @Override
    protected BinarySection materialize(TypeSerializer<ObjectRef> serializer) throws IOException {
        ObjectDescriptor descriptor = javaObject.toDescriptor();
        byte[] binaryString = StringData.fromString(descriptor.getURI()).toBytes();
        return new BinarySection(
                Arrays.asList(
                                MemorySegmentFactory.wrap(binaryString),
                                MemorySegmentFactory.wrapLong(descriptor.getOffset()),
                                MemorySegmentFactory.wrapLong(descriptor.getLength()))
                        .toArray(new MemorySegment[0]),
                0,
                binaryString.length + 16);
    }
}
