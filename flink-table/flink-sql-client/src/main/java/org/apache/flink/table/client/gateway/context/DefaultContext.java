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

package org.apache.flink.table.client.gateway.context;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.gateway.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

/**
 * Context describing default environment, command line options, flink config, etc.
 *
 * <p>When the {@link Executor} execute `reset` commands, the session can restore from the "default"
 * context.
 */
public class DefaultContext {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultContext.class);

    private final List<URL> dependencies;
    private final Configuration flinkConfig;

    public DefaultContext(List<URL> dependencies, Configuration flinkConfig) {
        this.dependencies = dependencies;
        this.flinkConfig = flinkConfig;
    }

    public Configuration getFlinkConfig() {
        return flinkConfig;
    }

    public List<URL> getDependencies() {
        return dependencies;
    }
}
