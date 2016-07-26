package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public abstract class JdbcTableSource<T> extends TableSource<T> implements JdbcExecutionOperator {

    public JdbcTableSource(String tableName, DataSetType<T> type) {
        super(tableName, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcTableSource(TableSource<T> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return this.getTableName();
    }
}
