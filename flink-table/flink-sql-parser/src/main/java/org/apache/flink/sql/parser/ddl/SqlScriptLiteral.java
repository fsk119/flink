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

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlScriptLiteral extends SqlLiteral {

    public static SqlScriptLiteral create(String value, SqlParserPos pos) {
        return new SqlScriptLiteral(new NlsString(value, null, null), pos);
    }

    protected SqlScriptLiteral(@Nullable Object value, SqlParserPos pos) {
        super(value, SqlTypeName.CHAR, pos);
    }

    @Override
    public SqlLiteral clone(SqlParserPos pos) {
        return new SqlScriptLiteral(value, pos);
    }

    @Override
    public String toString() {
        return "$$" + value + "$$";
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final NlsString nlsString = getValueAs(NlsString.class);
        writer.literal("$$" + nlsString.getValue() + "$$");
    }
}
