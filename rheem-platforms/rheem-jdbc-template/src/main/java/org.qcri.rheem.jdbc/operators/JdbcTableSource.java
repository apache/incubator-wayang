package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public abstract class JdbcTableSource extends TableSource<Record> implements JdbcExecutionOperator {

    public JdbcTableSource(String tableName) {
        super(tableName, DataSetType.createDefault(Record.class));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcTableSource(JdbcTableSource that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return this.getTableName();
    }
}
