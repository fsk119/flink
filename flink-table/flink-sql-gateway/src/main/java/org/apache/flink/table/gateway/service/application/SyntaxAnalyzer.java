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

import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.operations.Operation;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/** Split the script into statement list and analyze the statement one by one. */
public class SyntaxAnalyzer implements Iterator<Operation> {

    private final Supplier<TableEnvironmentInternal> tableEnvFactory;
    private final String statement;

    // these 3 string builders is here to pad the split sql to its original line and column
    // number
    StringBuilder previousPaddingSqlBuilder = new StringBuilder();
    StringBuilder currentPaddingSqlBuilder = new StringBuilder();
    StringBuilder currentPaddingLineBuilder = new StringBuilder();

    int position = 0;
    Operation operation;

    public SyntaxAnalyzer(Supplier<TableEnvironmentInternal> tableEnvFactory, String statement) {
        this.tableEnvFactory = tableEnvFactory;
        this.statement = statement;
    }

    @Override
    public boolean hasNext() {
        if (position >= statement.length()) {
            return false;
        }
        operation = analyzeStatements();
        return true;
    }

    @Override
    public Operation next() {
        return operation;
    }

    enum State {
        SINGLE_QUOTE, // '

        DOUBLE_QUOTE, // "

        BACK_QUOTE, // `

        SINGLE_COMMENT, // --

        MULTI_LINE_COMMENT, /* */

        NORMAL
    }

    private Operation analyzeStatements() {
        StringBuilder currentSqlBuilder = new StringBuilder();

        State state = State.NORMAL;
        char currentChar = 0;

        while (position < statement.length()) {
            char lastChar = currentChar;
            currentChar = statement.charAt(position);

            currentSqlBuilder.append(currentChar);
            currentPaddingLineBuilder.append(" ");

            switch (currentChar) {
                case '\'':
                    if (state == State.SINGLE_QUOTE) {
                        state = State.NORMAL;
                    } else if (state == State.NORMAL) {
                        state = State.SINGLE_QUOTE;
                    }
                    break;
                case '"':
                    if (state == State.DOUBLE_QUOTE) {
                        state = State.NORMAL;
                    } else if (state == State.NORMAL) {
                        state = State.DOUBLE_QUOTE;
                    }
                    break;
                case '`':
                    if (state == State.BACK_QUOTE) {
                        state = State.NORMAL;
                    } else if (state == State.NORMAL) {
                        state = State.BACK_QUOTE;
                    }
                    break;
                case '-':
                    if (lastChar == '-' && state == State.NORMAL) {
                        state = State.SINGLE_COMMENT;
                    }
                    break;
                case '\n':
                    if (state == State.SINGLE_COMMENT) {
                        state = State.NORMAL;
                    }
                    currentPaddingLineBuilder.setLength(0);
                    currentPaddingSqlBuilder.append("\n");
                    break;
                case '*':
                    if (lastChar == '/' && state == State.NORMAL) {
                        state = State.MULTI_LINE_COMMENT;
                    }
                    break;
                case '/':
                    if (lastChar == '*' && state == State.MULTI_LINE_COMMENT) {
                        state = State.NORMAL;
                    }
                    break;
                case ';':
                    if (state == State.NORMAL) {
                        position =
                                prefetch(
                                        statement,
                                        position + 1,
                                        currentSqlBuilder,
                                        currentPaddingSqlBuilder,
                                        currentPaddingLineBuilder);
                        String sql = currentSqlBuilder.toString();
                        try {
                            Operation operation = analyze(previousPaddingSqlBuilder + sql);
                            previousPaddingSqlBuilder.append(currentPaddingSqlBuilder);
                            previousPaddingSqlBuilder.append(currentPaddingLineBuilder);
                            currentPaddingSqlBuilder.setLength(0);
                            currentPaddingLineBuilder.setLength(0);
                            return operation;
                        } catch (SqlParserEOFException e) {
                            if (position == statement.length() - 1) {
                                throw e;
                            } else {
                                // keep reading
                                continue;
                            }
                        }


                    }
                    break;
                default:
                    break;
            }
            position++;
        }
        throw new SqlParserEOFException(currentSqlBuilder.toString());
    }

    private Operation analyze(String paddedStatement) {
        List<Operation> parsed = tableEnvFactory.get().getParser().parse(paddedStatement);
        return parsed.get(0);
    }

    /**
     * Prefetch characters until the character is not semicolon or white space.
     *
     * @param stmt the input script
     * @param begin the next token to fetch
     * @param currentSqlBuilder current fetched sql
     * @param currentPaddingSqlBuilder current padding script
     * @param currentPaddingLineBuilder the current padding line
     * @return the last fetched character
     */
    private int prefetch(
            String stmt,
            int begin,
            StringBuilder currentSqlBuilder,
            StringBuilder currentPaddingSqlBuilder,
            StringBuilder currentPaddingLineBuilder) {
        State state = State.NORMAL;
        char currentChar;
        for (int i = begin; i < stmt.length(); i++) {
            currentChar = stmt.charAt(i);
            char nextChar = i + 1 < stmt.length() ? stmt.charAt(i + 1) : currentChar;

            switch (currentChar) {
                case '-':
                    if (nextChar == '-' && state == State.NORMAL) {
                        state = State.SINGLE_COMMENT;
                    }
                    break;
                case '\n':
                    if (state == State.SINGLE_COMMENT) {
                        state = State.NORMAL;
                    }
                    break;
                case '*':
                    if (nextChar == '/' && state == State.MULTI_LINE_COMMENT) {
                        state = State.NORMAL;
                        currentSqlBuilder.append("*/");
                        currentPaddingLineBuilder.append("  ");
                        i = i + 1;
                        continue;
                    }
                    break;
                case '/':
                    if (nextChar == '*' && state == State.NORMAL) {
                        state = State.MULTI_LINE_COMMENT;
                    }
                    break;
            }

            if (state == State.NORMAL
                    && currentChar != ';'
                    && !Character.isWhitespace(currentChar)) {
                return i;
            }

            currentSqlBuilder.append(currentChar);
            if (currentChar == '\n') {
                currentPaddingLineBuilder.setLength(0);
                currentPaddingSqlBuilder.append("\n");
            } else {
                currentPaddingLineBuilder.append(" ");
            }
        }
        return stmt.length();
    }
}
