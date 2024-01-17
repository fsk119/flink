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

package org.apache.flink.table;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.TemporaryClassLoaderContext;

import java.io.IOException;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/** Driver. */
public class SqlDriver {

    private static final String RUNNER_CLASS_NAME =
            "org.apache.flink.table.gateway.service.application.SqlScriptRunner";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException(
                    "It should never happen. Please configure statements.");
        }
        try (MutableURLClassLoader executorLoader =
                        FlinkUserCodeClassLoaders.create(
                                new URL[] {findExecutor().toUri().toURL()},
                                Thread.currentThread().getContextClassLoader(),
                                new Configuration());
                TemporaryClassLoaderContext ignored =
                        TemporaryClassLoaderContext.of(executorLoader)) {
            executorLoader
                    .loadClass(RUNNER_CLASS_NAME)
                    .getMethod("run", String.class)
                    .invoke(null, args[0]);
        }
    }

    private static Path findExecutor() {
        String flinkOptPath = System.getenv(ConfigConstants.ENV_FLINK_OPT_DIR);
        final List<Path> sqlJarPaths = new ArrayList<>();
        try {
            Files.walkFileTree(
                    FileSystems.getDefault().getPath(flinkOptPath),
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                throws IOException {
                            FileVisitResult result = super.visitFile(file, attrs);
                            if (file.getFileName().toString().startsWith("flink-sql-gateway")) {
                                sqlJarPaths.add(file);
                            }
                            return result;
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(
                    "Exception encountered during finding the flink-agent jar. This should not happen.",
                    e);
        }

        if (sqlJarPaths.size() != 1) {
            throw new RuntimeException("Found " + sqlJarPaths.size() + " flink-sql-gateway jar.");
        }

        return sqlJarPaths.get(0);
    }
}
