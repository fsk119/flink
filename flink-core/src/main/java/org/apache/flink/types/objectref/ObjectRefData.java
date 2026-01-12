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

import java.util.Objects;

public class ObjectRefData implements ObjectRef {

    private final ObjectDescriptor descriptor;

    public static ObjectRefData createEmptyInstance() {
        return new ObjectRefData(null);
    }

    public ObjectRefData(ObjectDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public ObjectDescriptor toDescriptor() {
        return descriptor;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ObjectRefData)) {
            return false;
        }
        ObjectRefData that = (ObjectRefData) object;
        return that.descriptor.equals(descriptor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptor);
    }
}
