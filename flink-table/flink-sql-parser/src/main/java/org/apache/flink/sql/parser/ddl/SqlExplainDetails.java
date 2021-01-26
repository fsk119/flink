package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Set;

/** Sql explain. */
public class SqlExplainDetails extends SqlCall {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("EXPLAIN OPERATOR", SqlKind.EXPLAIN);
    SqlNode statement;
    Set<String> details;

    public SqlExplainDetails(SqlParserPos pos, SqlNode statement, Set<String> details) {
        super(pos);
        this.statement = statement;
        this.details = details;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(statement);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("EXPLAIN");
        if (details.isEmpty()) {
            writer.keyword("PLAN FOR");
        } else {
            for (String detail : details) {
                writer.keyword(detail);
            }
        }
        statement.unparse(writer, leftPrec, rightPrec);
    }
}
