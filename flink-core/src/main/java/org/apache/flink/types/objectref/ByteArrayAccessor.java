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

import org.apache.flink.configuration.MemorySize;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class ByteArrayAccessor implements ObjectAccessor {

    private final byte[] bytes;

    public ByteArrayAccessor(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public CompletableFuture<byte[]> getBytes() {
        return CompletableFuture.completedFuture(bytes);
    }

    @Override
    public MemorySize getSize() {
        return new MemorySize(bytes.length);
    }

    @Override
    public ObjectAccessor copy() {
        return new ByteArrayAccessor(Arrays.copyOf(bytes, bytes.length));
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ByteArrayAccessor)) {
            return false;
        }
        ByteArrayAccessor that = (ByteArrayAccessor) object;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
