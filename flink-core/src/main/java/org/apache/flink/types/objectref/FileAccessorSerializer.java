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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class FileAccessorSerializer extends TypeSerializer<FileAccessor> {

    public static final FileAccessorSerializer INSTANCE = new FileAccessorSerializer();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<FileAccessor> duplicate() {
        return INSTANCE;
    }

    @Override
    public FileAccessor createInstance() {
        return new FileAccessor(new Path());
    }

    @Override
    public FileAccessor copy(FileAccessor from) {
        return new FileAccessor(from.getPath());
    }

    @Override
    public FileAccessor copy(FileAccessor from, FileAccessor reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(FileAccessor record, DataOutputView target) throws IOException {
        StringSerializer.INSTANCE.serialize(record.getPath().toString(), target);
    }

    @Override
    public FileAccessor deserialize(DataInputView source) throws IOException {
        return new FileAccessor(new Path(StringSerializer.INSTANCE.deserialize(source)));
    }

    @Override
    public FileAccessor deserialize(FileAccessor reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        StringSerializer.INSTANCE.copy(source, target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FileAccessorSerializer;
    }

    @Override
    public int hashCode() {
        return FileAccessorSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<FileAccessor> snapshotConfiguration() {
        return new FileAccessorSerializerSnapshot();
    }

    public static class FileAccessorSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<FileAccessor> {

        public FileAccessorSerializerSnapshot() {
            super(() -> FileAccessorSerializer.INSTANCE);
        }
    }
}
