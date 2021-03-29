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
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_RESET_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SET_KEY;
import static org.apache.flink.table.client.config.YamlConfigUtils.getPropertiesInPretty;

/** Executor to execute commands. */
public abstract class AbstractSqlCommandExecutor {

    protected final SqlCommandPrinter printer;
    protected final Executor executor;
    protected final String sessionId;

    public AbstractSqlCommandExecutor(
            SqlCommandPrinter printer, Executor executor, String sessionId) {
        this.printer = printer;
        this.executor = executor;
        this.sessionId = sessionId;
    }

    public Optional<Operation> parseCommand(String stmt) {
        // normalize
        stmt = stmt.trim();
        // remove ';' at the end
        if (stmt.endsWith(";")) {
            stmt = stmt.substring(0, stmt.length() - 1).trim();
        }

        Operation operation = executor.parseStatement(sessionId, stmt);
        return Optional.of(operation);
    }

    public void callOperation(Operation operation) {
        if (operation instanceof QuitOperation) {
            // QUIT/EXIT
            callQuit();
        } else if (operation instanceof ClearOperation) {
            // CLEAR
            callClear();
        } else if (operation instanceof HelpOperation) {
            // HELP
            callHelp();
        } else if (operation instanceof SetOperation) {
            // SET
            callSet((SetOperation) operation);
        } else if (operation instanceof ResetOperation) {
            // RESET
            callReset((ResetOperation) operation);
        } else if (operation instanceof CatalogSinkModifyOperation) {
            // INSERT INTO/OVERWRITE
            callInsert((CatalogSinkModifyOperation) operation);
        } else if (operation instanceof QueryOperation) {
            // SELECT
            callSelect((QueryOperation) operation);
        } else {
            // fallback to default implementation
            executeOperation(operation);
        }
    }

    protected abstract void callQuit();

    protected abstract void callClear();

    protected abstract void callHelp();

    protected abstract void callInsert(CatalogSinkModifyOperation operation);

    protected abstract void callSelect(QueryOperation queryOperation);

    protected void callSet(SetOperation setOperation) {
        // set a property
        if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
            String key = setOperation.getKey().get().trim();
            String value = setOperation.getValue().get().trim();
            executor.setSessionProperty(sessionId, key, value);
            printer.printSetResetConfigKeyMessage(key, MESSAGE_SET_KEY);
        }
        // show all properties
        else {
            final Map<String, String> properties = executor.getSessionConfigMap(sessionId);
            if (properties.isEmpty()) {
                printer.printInfo(CliStrings.MESSAGE_EMPTY);
            } else {
                List<String> prettyEntries = getPropertiesInPretty(properties);
                prettyEntries.forEach(printer::printMessage);
            }
            printer.flush();
        }
    }

    protected void callReset(ResetOperation resetOperation) {
        // reset all session properties
        if (!resetOperation.getKey().isPresent()) {
            executor.resetSessionProperties(sessionId);
            printer.printInfo(CliStrings.MESSAGE_RESET);
        }
        // reset a session property
        else {
            String key = resetOperation.getKey().get();
            executor.resetSessionProperty(sessionId, key);
            printer.printSetResetConfigKeyMessage(key, MESSAGE_RESET_KEY);
        }
    }

    protected void executeOperation(Operation operation) {
        TableResult result = executor.executeOperation(sessionId, operation);
        printer.printTableResult(result);
    }

    public Executor getExecutor() {
        return executor;
    }
}
