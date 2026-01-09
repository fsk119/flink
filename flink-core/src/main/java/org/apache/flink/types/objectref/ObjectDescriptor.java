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

import java.io.Serializable;
import java.net.URI;
import java.util.Objects;

public class ObjectDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long LENGTH_TO_END = -1;

    private final String uri;
    private final long offset;
    private final long length;

    public static ObjectDescriptor createEmptyInstance() {
        return new ObjectDescriptor("", 0, 0);
    }

    public ObjectDescriptor(String uri) {
        this.uri = uri;
        this.offset = 0;
        this.length = LENGTH_TO_END;
    }

    public ObjectDescriptor(String uri, long offset, long length) {
        this.uri = uri;
        this.offset = offset;
        this.length = length;
    }

    public String getURI() {
        return uri;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ObjectDescriptor)) {
            return false;
        }
        ObjectDescriptor that = (ObjectDescriptor) object;
        return offset == that.offset && length == that.length && Objects.equals(uri, that.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, offset, length);
    }
}
