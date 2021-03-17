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

package org.apache.flink.table.planner.regex;

import org.apache.flink.table.operations.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Regex parser. */
public class RegexParser {

    public static final RegexParser INSTANCE = new RegexParser();

    private static final List<RegexStrategy> REGEX_STRATEGIES =
            Arrays.asList(
                    (RegexStrategy) HelpOperationStrategy.INSTANCE, QuitOperationStrategy.INSTANCE);

    public Optional<Operation> convert(String statement) {
        for (RegexStrategy strategy : REGEX_STRATEGIES) {
            if (strategy.match(statement)) {
                return Optional.of(strategy.convert(statement));
            }
        }
        return Optional.empty();
    }

    /** Maybe we should introduce another class, e.g `CommandAdvisor` to clarify the concepts. */
    public String[] getCompletionHints(String statement, int cursor) {
        List<String> hints = new ArrayList<>();
        for (RegexStrategy strategy : REGEX_STRATEGIES) {
            for (String hint : strategy.getHints()) {
                if (hint.startsWith(statement) && cursor < hint.length()) {
                    hints.add(hint);
                }
            }
        }
        return hints.toArray(new String[0]);
    }
}
