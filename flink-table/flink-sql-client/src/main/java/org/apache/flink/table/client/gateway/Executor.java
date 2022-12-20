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

package org.apache.flink.table.client.gateway;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.SqlParserEOFException;

import javax.annotation.Nullable;

import java.net.URL;
import java.util.List;

/** A gateway for communicating with Flink and other external systems. */
public interface Executor {

    /**
     * Open a new session by using the given session id.
     *
     * @param sessionId session identifier.
     * @throws SqlExecutionException if any error happen
     */
    void open(@Nullable String sessionId) throws SqlExecutionException;

    /**
     * Close the resources of session for given session id.
     *
     * @throws SqlExecutionException if any error happen
     */
    void close() throws SqlExecutionException;

    /**
     * Returns a {@link ReadableConfig} of all session configurations that are defined by the
     * executor and the session.
     */
    ReadableConfig getSessionConfig() throws SqlExecutionException;

    /**
     * Reset all the properties for the given session identifier.
     *
     * @throws SqlExecutionException if any error happen.
     */
    void resetSessionProperties() throws SqlExecutionException;

    /**
     * Reset given key's the session property for default value, if key is not defined in config
     * file, then remove it.
     *
     * @param key of need to reset the session property
     * @throws SqlExecutionException if any error happen.
     */
    void resetSessionProperty(String key) throws SqlExecutionException;

    /**
     * Set given key's session property to the specific value.
     *
     * @param key of the session property
     * @param value of the session property
     * @throws SqlExecutionException if any error happen.
     */
    void setSessionProperty(String key, String value) throws SqlExecutionException;

    ClientResult executeStatement(String statement)
            throws SqlExecutionException, SqlParserEOFException;

    default void configureStatement(String statement) throws SqlExecutionException {
        throw new UnsupportedOperationException("Not implemented.");
    }

    /** Returns a list of completion hints for the given statement at the given position. */
    List<String> completeStatement(String statement, int position);

    default void addJar(URL dependency) throws SqlExecutionException {
        throw new UnsupportedOperationException();
    }
}
