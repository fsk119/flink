/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.gateway.ClientResult;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.local.result.ChangelogResult;
import org.apache.flink.table.client.gateway.local.result.DynamicResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedResult;

import org.jline.reader.MaskingCallback;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedString;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Contains basic tests for the {@link CliResultView}. */
class CliResultViewTest {

    private static final String SESSION_ID = "test-session";

    @Test
    void testTableResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(TypedResult.endOfStream(), true, 0);
    }

    @Test
    void testTableResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(TypedResult.empty(), true, 1);
    }

    @Test
    void testTableResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(TypedResult.payload(1), true, 1);
    }

    @Test
    void testChangelogResultViewKeepJobResult() throws Exception {
        testResultViewClearResult(TypedResult.endOfStream(), false, 0);
    }

    @Test
    void testChangelogResultViewClearEmptyResult() throws Exception {
        testResultViewClearResult(TypedResult.empty(), false, 1);
    }

    @Test
    void testChangelogResultViewClearPayloadResult() throws Exception {
        testResultViewClearResult(TypedResult.payload(Collections.emptyList()), false, 1);
    }

    private static class MockedExecutor implements Executor {

        @Override
        public void open(@org.jetbrains.annotations.Nullable String sessionId)
                throws SqlExecutionException {
            // do nothing
        }

        @Override
        public void close() throws SqlExecutionException {
            // do nothing
        }

        @Override
        public ReadableConfig getSessionConfig() throws SqlExecutionException {
            return null;
        }

        @Override
        public void resetSessionProperties() throws SqlExecutionException {}

        @Override
        public void resetSessionProperty(String key) throws SqlExecutionException {}

        @Override
        public void setSessionProperty(String key, String value) throws SqlExecutionException {}

        @Override
        public ClientResult executeStatement(String statement)
                throws SqlExecutionException, SqlParserEOFException {
            return null;
        }

        @Override
        public List<String> completeStatement(String statement, int position) {
            return null;
        }
    }

    private void testResultViewClearResult(
            TypedResult<?> typedResult, boolean isTableMode, int expectedCancellationCount)
            throws Exception {
        final CountDownLatch cancellationCounterLatch =
                new CountDownLatch(expectedCancellationCount);
        final Configuration testConfig = new Configuration();
        testConfig.set(EXECUTION_RESULT_MODE, ResultMode.TABLE);
        testConfig.set(RUNTIME_MODE, RuntimeExecutionMode.STREAMING);

        try (CliClient cli =
                new TestingCliClient(
                        TerminalUtils.createDumbTerminal(),
                        new MockedExecutor(),
                        File.createTempFile("history", "tmp").toPath(),
                        null)) {
            Thread resultViewRunner = new Thread(new TestingCliResultView(cli, isTableMode, null));
            resultViewRunner.start();

            if (!resultViewRunner.isInterrupted()) {
                resultViewRunner.interrupt();
            }
            // close the client until view exit
            while (resultViewRunner.isAlive()) {
                Thread.sleep(100);
            }
        }

        assertThat(cancellationCounterLatch.await(10, TimeUnit.SECONDS))
                .as("Invalid number of cancellations.")
                .isTrue();
    }

    private static final class TestingCliResultView implements Runnable {

        private final CliResultView realResultView;

        public TestingCliResultView(CliClient client, boolean isTableMode, DynamicResult result) {

            if (isTableMode) {
                realResultView = new TestingCliTableResultView(client, (MaterializedResult) result);
            } else {
                realResultView =
                        new TestingCliChangelogResultView(client, (ChangelogResult) result);
            }
        }

        @Override
        public void run() {
            realResultView.open();
        }
    }

    private static class TestingCliChangelogResultView extends CliChangelogResultView {

        public TestingCliChangelogResultView(CliClient client, ChangelogResult result) {
            super(client, result);
        }

        @Override
        protected List<AttributedString> computeMainHeaderLines() {
            return Collections.emptyList();
        }
    }

    private static class TestingCliTableResultView extends CliTableResultView {

        public TestingCliTableResultView(CliClient client, MaterializedResult result) {
            super(client, result);
        }

        @Override
        protected List<AttributedString> computeMainHeaderLines() {
            return Collections.emptyList();
        }
    }

    private static class TestingCliClient extends CliClient {

        private final Terminal terminal;

        public TestingCliClient(
                Terminal terminal,
                Executor executor,
                Path historyFilePath,
                @Nullable MaskingCallback inputTransformer) {
            super(() -> terminal, executor, historyFilePath, inputTransformer);
            this.terminal = terminal;
        }

        @Override
        public Terminal getTerminal() {
            return terminal;
        }

        @Override
        public boolean isPlainTerminal() {
            return true;
        }

        @Override
        public void clearTerminal() {
            // do nothing
        }
    }
}
