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

/** Exception thrown during the execution of SQL statements. */
public class SqlExecutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ExceptionType type = ExceptionType.SQL_EXECUTE_ERROR;

    public SqlExecutionException(String message) {
        super(message);
    }

    public SqlExecutionException(ExceptionType type, String message) {
        super(message);
        this.type = type;
    }

    public SqlExecutionException(String message, Throwable e) {
        super(message, e);
    }

    public SqlExecutionException(ExceptionType type, String message, Throwable e) {
        super(message, e);
        this.type = type;
    }

    public ExceptionType getType() {
        return type;
    }

    /** Exception type is used to indicate the specific type of error. */
    public enum ExceptionType {
        SQL_PARSE_ERROR,
        SQL_EXECUTE_ERROR
    }
}
