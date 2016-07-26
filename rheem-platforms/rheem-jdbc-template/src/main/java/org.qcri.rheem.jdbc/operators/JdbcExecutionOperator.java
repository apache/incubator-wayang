package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;

public interface JdbcExecutionOperator extends ExecutionOperator {

    /**
     * Creates a SQL clause for this instance. For {@link TableSource}s it returns an identifier for the table
     * usable in a {@code FROM} clause. For {@link ProjectionOperator}s it returns a list usable in a
     * {@code SELECT} clause. For {@link FilterOperator}s it creates a condition usable in a {@code WHERE} clause.
     * Also, these different clauses should be compatible for connected {@link JdbcExecutionOperator}s.
     *
     * @param compiler used to create SQL code
     * @return the SQL clause
     */
    String createSqlClause(Connection connection, FunctionCompiler compiler);

}