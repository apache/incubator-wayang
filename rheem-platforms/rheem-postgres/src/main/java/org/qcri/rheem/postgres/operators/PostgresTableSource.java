package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public class PostgresTableSource extends TableSource implements PostgresExecutionOperator {

    public PostgresTableSource(String tableName, DataSetType type) {
        super(tableName, type);
    }

    @Override
    public String evaluate(ChannelInstance[] inputChannels, ChannelInstance[] outputChannels, FunctionCompiler compiler) {
        return "select %s from " + this.getTableName();
    }
}
