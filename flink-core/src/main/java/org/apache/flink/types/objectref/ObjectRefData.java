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
import java.util.Objects;

public class ObjectRefData implements ObjectRef {

    private static final long serialVersionUID = 1L;

    private final ObjectAccessor accessor;
    private final String contentType;

    public static ObjectRefData createEmptyInstance() {
        return new ObjectRefData("application/octet-stream", new ByteArrayAccessor(new byte[0]));
    }

    public ObjectRefData(String contentType, ObjectAccessor accessor) {
        this.accessor = accessor;
        this.contentType = contentType;
    }

    /** Accessor to get the object */
    public ObjectAccessor getAccessor() {
        return accessor;
    }

    /**
     * Describe the object type. It describes media types.
     *
     * <p>https://www.iana.org/assignments/media-types/media-types.xhtml
     */
    public String getContentType() {
        return contentType;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ObjectRefData)) {
            return false;
        }
        ObjectRefData objectRefData = (ObjectRefData) object;
        return Objects.equals(accessor, objectRefData.accessor)
                && Objects.equals(contentType, objectRefData.contentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessor, contentType);
    }
}
