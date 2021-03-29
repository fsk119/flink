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

import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_DEPRECATED_KEY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_REMOVED_KEY;
import static org.apache.flink.table.client.config.YamlConfigUtils.getOptionNameWithDeprecatedKey;
import static org.apache.flink.table.client.config.YamlConfigUtils.isDeprecatedKey;
import static org.apache.flink.table.client.config.YamlConfigUtils.isRemovedKey;

/** SqlCommandPrinter. */
public interface SqlCommandPrinter extends AutoCloseable {

    void printMessage(CharSequence message);

    void printWarning(String message);

    void printInfo(String message);

    void printTableResult(TableResult tableResult);

    void printExecutionException(Throwable t, boolean isVerbose);

    void flush();

    default void printSetResetConfigKeyMessage(String key, String message) {
        boolean isRemovedKey = isRemovedKey(key);
        boolean isDeprecatedKey = isDeprecatedKey(key);

        // print warning information if the given key is removed or deprecated
        if (isRemovedKey || isDeprecatedKey) {
            String warningMsg =
                    isRemovedKey
                            ? MESSAGE_REMOVED_KEY
                            : String.format(
                                    MESSAGE_DEPRECATED_KEY,
                                    key,
                                    getOptionNameWithDeprecatedKey(key));
            printWarning(warningMsg);
        }

        // when the key is not removed, need to print normal message
        if (!isRemovedKey) {
            printInfo(message);
        }
    }

    static SqlCommandPrinter getBlackHolePrinter() {
        return new SqlCommandPrinter() {
            @Override
            public void close() throws Exception {}

            @Override
            public void printMessage(CharSequence message) {}

            @Override
            public void printWarning(String message) {}

            @Override
            public void printInfo(String message) {}

            @Override
            public void printTableResult(TableResult tableResult) {}

            @Override
            public void printExecutionException(Throwable t, boolean isVerbose) {}

            @Override
            public void flush() {}
        };
    }
}
