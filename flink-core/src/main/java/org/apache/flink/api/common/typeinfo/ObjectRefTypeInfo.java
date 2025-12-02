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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ObjectRefSerializer;
import org.apache.flink.types.objectref.ObjectRef;

public class ObjectRefTypeInfo extends TypeInformation<ObjectRef> {

    public static final ObjectRefTypeInfo INSTANCE = new ObjectRefTypeInfo();

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<ObjectRef> getTypeClass() {
        return ObjectRef.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<ObjectRef> createSerializer(SerializerConfig config) {
        return new ObjectRefSerializer();
    }

    @Override
    public String toString() {
        return ObjectRef.class.getSimpleName();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ObjectRefTypeInfo;
    }

    @Override
    public int hashCode() {
        return ObjectRefTypeInfo.class.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof ObjectRefTypeInfo;
    }
}
