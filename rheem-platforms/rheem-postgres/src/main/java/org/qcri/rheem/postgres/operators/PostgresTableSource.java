package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;

/**
 * Created by yidris on 3/22/16.
 */
public class PostgresTableSource extends TableSource implements PostgresExecutionOperator {

    public PostgresTableSource(String tableName, DataSetType type) {
        super(tableName, type);
    }

    @Override
    public String evaluate(Channel[] inputChannels, Channel[] outputChannels, FunctionCompiler compiler) {
        return "select %s from " + this.getTableName();
    }
}
