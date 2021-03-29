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

package org.apache.flink.table.client.cli;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.utils.PrintUtils;

import org.jline.terminal.Terminal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.table.api.internal.TableResultImpl.TABLE_RESULT_OK;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_EXECUTE_STATEMENT;

/** Terminal. */
public class TerminalPrinter implements SqlCommandPrinter {

    private static final Logger LOG = LoggerFactory.getLogger(TerminalPrinter.class);

    private final Terminal terminal;

    public TerminalPrinter(Terminal terminal) {
        this.terminal = terminal;
    }

    public Terminal getTerminal() {
        return terminal;
    }

    @Override
    public void printMessage(CharSequence message) {
        terminal.writer().println(message);
    }

    @Override
    public void printWarning(String message) {
        terminal.writer().println(CliStrings.messageWarning(message).toAnsi());
        terminal.flush();
    }

    @Override
    public void printInfo(String message) {
        terminal.writer().println(CliStrings.messageInfo(message).toAnsi());
        terminal.flush();
    }

    @Override
    public void printTableResult(TableResult tableResult) {
        if (TABLE_RESULT_OK == tableResult) {
            // print more meaningful message than tableau OK result
            printInfo(MESSAGE_EXECUTE_STATEMENT);
        } else {
            // print tableau if result has content
            PrintUtils.printAsTableauForm(
                    tableResult.getResolvedSchema(),
                    tableResult.collect(),
                    terminal.writer(),
                    Integer.MAX_VALUE,
                    "",
                    false,
                    false);
            terminal.flush();
        }
    }

    @Override
    public void flush() {
        terminal.flush();
    }

    @Override
    public void printExecutionException(Throwable t, boolean isVerbose) {
        final String errorMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
        LOG.warn(errorMessage, t);
        terminal.writer().println(CliStrings.messageError(errorMessage, t, isVerbose).toAnsi());
        terminal.flush();
    }

    @Override
    public void close() {
        try {
            terminal.close();
        } catch (IOException e) {
            throw new SqlClientException("Unable to close terminal.", e);
        }
    }
}
