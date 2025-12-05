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

package org.apache.flink.table.data;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava33.com.google.common.cache.LoadingCache;

import java.lang.ref.Cleaner;
import java.util.concurrent.ConcurrentHashMap;

public class DynamicTypeRegistry {

    private final ConcurrentHashMap<ClassLoader, LoadingCache<String, TypeSerializer>> caches =
            new ConcurrentHashMap<>();

    private static final Cleaner CLEANER = Cleaner.create();
    private static final DynamicTypeRegistry INSTANCE = new DynamicTypeRegistry();

    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> getSerializer(ClassLoader loader, String serializerName) {
        return caches.computeIfAbsent(
                        loader,
                        cls -> {
                            CLEANER.register(loader, () -> caches.remove(loader));
                            return CacheBuilder.newBuilder()
                                    .softValues()
                                    .build(
                                            new CacheLoader<String, TypeSerializer>() {
                                                @Override
                                                public TypeSerializer load(String name) {
                                                    try {
                                                        return InstantiationUtil.instantiate(
                                                                name, TypeSerializer.class, loader);
                                                    } catch (FlinkException e) {
                                                        throw new RuntimeException(
                                                                "Failed to create the serializer.",
                                                                e);
                                                    }
                                                }
                                            });
                        })
                .getUnchecked(serializerName);
    }

    public static DynamicTypeRegistry getInstance() {
        return INSTANCE;
    }
}
