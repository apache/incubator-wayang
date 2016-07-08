package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public class PostgresTableSource<T> extends TableSource<T> implements PostgresExecutionOperator {

    public PostgresTableSource(String tableName, DataSetType<T> type) {
        super(tableName, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PostgresTableSource(TableSource<T> that) {
        super(that);
    }

    @Override
    public String evaluate(ChannelInstance[] inputChannels, ChannelInstance[] outputChannels, FunctionCompiler compiler) {
        return "select %s from " + this.getTableName();
    }
}
