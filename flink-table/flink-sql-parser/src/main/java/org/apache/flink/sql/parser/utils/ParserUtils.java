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

package org.apache.flink.sql.parser.utils;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants;

import org.apache.calcite.sql.parser.SqlParserUtil;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils for keywords. */
public class ParserUtils {

    public static Set<String> getKeywords() {
        Set<String> keywords =
                Arrays.stream(FlinkSqlParserImplConstants.tokenImage)
                        .map(SqlParserUtil::getTokenVal)
                        // Ignore EOF
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
        Set<String> nonReservedKeywords = new HashSet<>();
        try (FileInputStream in =
                new FileInputStream(
                        ParserUtils.class
                                .getClassLoader()
                                .getResource("NonReservedKeywords.properties")
                                .getFile())) {
            Properties properties = new Properties();
            properties.load(in);

            nonReservedKeywords =
                    Arrays.stream(properties.getProperty("nonReservedKeywords").split(","))
                            .map(String::trim)
                            .collect(Collectors.toSet());
        } catch (Exception e) {
            e.printStackTrace();
        }
        keywords.removeAll(nonReservedKeywords);
        return keywords;
    }
}
