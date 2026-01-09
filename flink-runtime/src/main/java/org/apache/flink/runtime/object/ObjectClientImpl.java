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

package org.apache.flink.runtime.object;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.object.ObjectClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

// BlobStore is binds with a specific fs.
public class ObjectClientImpl implements Closeable, ObjectClient {

    private final JobID jobId;
    private final Map<String, FileIOFactory> factories;
    private final Map<CacheKey, FileIO> fileIOMap;

    public static ObjectClient create(
            JobID jobId, Configuration jobConfiguration, @Nullable PluginManager pluginManager) {
        return new ObjectClientImpl(
                jobId,
                initializeFactories(
                        new UnmodifiableConfiguration(jobConfiguration), pluginManager));
    }

    ObjectClientImpl(JobID jobId, Map<String, FileIOFactory> factories) {
        this.jobId = jobId;
        this.factories = factories;
        this.fileIOMap = new ConcurrentHashMap<>();
    }

    private static Map<String, FileIOFactory> initializeFactories(
            Configuration jobConfiguration, @Nullable PluginManager pluginManager) {
        // Load and initialize FileSystem factories following the same pattern as FileSystem class
        Collection<Supplier<Iterator<FileSystemFactory>>> factorySuppliers = new ArrayList<>(2);
        factorySuppliers.add(() -> ServiceLoader.load(FileSystemFactory.class).iterator());
        Map<String, FileIOFactory> factories = new ConcurrentHashMap<>();
        if (pluginManager != null) {
            factorySuppliers.add(() -> pluginManager.load(FileSystemFactory.class));
        }

        final List<FileSystemFactory> fileSystemFactories =
                loadFileSystemFactories(factorySuppliers);

        // Configure all file system factories and populate the factories map
        for (FileSystemFactory factory : fileSystemFactories) {
            // Apply configuration to the factory
            try {
                factory.configure(jobConfiguration);
            } catch (Exception e) {
                // Log the error but continue with other factories
                throw new RuntimeException(
                        "Failed to configure: " + factory.getClass().getName(), e);
            }

            String scheme = factory.getScheme();
            // Register the factory with the scheme
            factories.put(scheme, new FileSystemIOFactory(factory));
        }

        return factories;
    }

    public InputStream getObject(Path path) throws IOException {
        URI uri = path.toUri();
        if (uri.getScheme() == null) {
            return FileSystem.getLocalFileSystem().open(path);
        }
        if (!factories.containsKey(uri.getScheme())) {
            throw new UnsupportedOperationException(
                    "Unsupported scheme: "
                            + uri.getScheme()
                            + ". Please make sure that the scheme is supported by the FileSystemFactory.");
        }

        return fileIOMap
                .computeIfAbsent(
                        new CacheKey(uri.getScheme(), uri.getAuthority()),
                        key -> {
                            FileIOFactory factory = factories.get(uri.getScheme());
                            ClassLoader classLoader = factory.getClass().getClassLoader();
                            try (TemporaryClassLoaderContext ignored =
                                    TemporaryClassLoaderContext.of(classLoader)) {
                                return factory.create(path);
                            }
                        })
                .read(path);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public InputStream open(String path) throws IOException {
        URI uri = URI.create(path);
        Path flinkPath = new Path(path);
        if (uri.getScheme() == null) {
            return FileSystem.getLocalFileSystem().open(flinkPath);
        }
        if (!factories.containsKey(uri.getScheme())) {
            throw new UnsupportedOperationException(
                    "Unsupported scheme: "
                            + uri.getScheme()
                            + ". Please make sure that the scheme is supported by the FileSystemFactory.");
        }

        return fileIOMap
                .computeIfAbsent(
                        new CacheKey(uri.getScheme(), uri.getAuthority()),
                        key -> {
                            FileIOFactory factory = factories.get(uri.getScheme());
                            ClassLoader classLoader = factory.getClass().getClassLoader();
                            try (TemporaryClassLoaderContext ignored =
                                    TemporaryClassLoaderContext.of(classLoader)) {
                                return factory.create(flinkPath);
                            }
                        })
                .read(flinkPath);
    }

    interface FileIOFactory {

        FileIO create(Path path);
    }

    interface FileIO {

        InputStream read(Path path);
    }

    static class FileSystemIOFactory implements FileIOFactory {

        private final FileSystemFactory fileSystemFactory;

        private FileSystemIOFactory(FileSystemFactory fileSystemFactory) {
            this.fileSystemFactory = fileSystemFactory;
        }

        @Override
        public FileIO create(Path path) {
            try {
                return new FileSystemFileIO(fileSystemFactory.create(path.toUri()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static class FileSystemFileIO implements FileIO {

        private final FileSystem fileSystem;

        private FileSystemFileIO(FileSystem fileSystem) {
            this.fileSystem = fileSystem;
        }

        @Override
        public InputStream read(Path path) {
            try {
                return fileSystem.open(path);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class CacheKey implements Serializable {
        private final String scheme;
        private final String authority;

        private CacheKey(String scheme, String authority) {
            this.scheme = scheme;
            this.authority = authority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(scheme, cacheKey.scheme)
                    && Objects.equals(authority, cacheKey.authority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scheme, authority);
        }
    }

    /**
     * Loads the factories for the file systems directly supported by Flink. Following the same
     * pattern as FileSystem class.
     *
     * @return A list of file system factories.
     */
    private static List<FileSystemFactory> loadFileSystemFactories(
            Collection<Supplier<Iterator<FileSystemFactory>>> factoryIteratorsSuppliers) {

        final ArrayList<FileSystemFactory> list = new ArrayList<>();

        // Load factories from different suppliers
        for (Supplier<Iterator<FileSystemFactory>> factoryIteratorsSupplier :
                factoryIteratorsSuppliers) {
            try {
                addAllFactoriesToList(factoryIteratorsSupplier.get(), list);
            } catch (Throwable t) {
                // catching Throwable here to handle various forms of class loading
                // and initialization errors
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                // Log the error but continue with other suppliers
                System.err.println(
                        "Failed to load additional file systems via services: " + t.getMessage());
            }
        }

        return Collections.unmodifiableList(list);
    }

    private static void addAllFactoriesToList(
            Iterator<FileSystemFactory> iter, List<FileSystemFactory> list) {
        // we explicitly use an iterator here (rather than for-each) because that way
        // we can catch errors in individual service instantiations

        while (iter.hasNext()) {
            try {
                FileSystemFactory factory = iter.next();
                list.add(factory);
            } catch (Throwable t) {
                // catching Throwable here to handle various forms of class loading
                // and initialization errors
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
                System.err.println("Failed to load a file system via services: " + t.getMessage());
            }
        }
    }
}
