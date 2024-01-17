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

package org.apache.flink.table.gateway.service.application;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.Executors;

/** Runner to run the script. */
public class SqlScriptRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SqlScriptRunner.class);

    public static void run(String script) throws Exception {
        // CC {@link ClientUtils#executeProgram }
        Configuration configuration =
                (Configuration)
                        StreamExecutionEnvironment.getExecutionEnvironment(new Configuration())
                                .getConfiguration();
        SessionContext sessionContext =
                SessionContext.create(
                        DefaultContext.load(configuration, Collections.emptyList(), true),
                        SessionHandle.create(),
                        SessionEnvironment.newBuilder().build(),
                        Executors.newSingleThreadExecutor());

        SyntaxAnalyzer syntaxAnalyzer =
                new SyntaxAnalyzer(
                        () ->
                                sessionContext
                                        .createOperationExecutor(new Configuration())
                                        .getTableEnvironment(),
                        script);

        while (syntaxAnalyzer.hasNext()) {
            Operation operation = syntaxAnalyzer.next();
            if (operation instanceof SetOperation || operation instanceof ResetOperation) {
                throw new UnsupportedOperationException();
            } else if (operation instanceof QueryOperation) {
                throw new UnsupportedOperationException();
            }
            sessionContext
                    .createOperationExecutor(new Configuration())
                    .getTableEnvironment()
                    .executeInternal(operation);
        }
    }
}
