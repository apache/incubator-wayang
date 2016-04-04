package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.plan.executionplan.Channel;

/**
 * Created by yidris on 3/22/16.
 */
public class PostgresTableSource extends TableSource implements PostgresExecutionOperator {

    public PostgresTableSource(String tableName) {
        super(tableName);
    }

    @Override
    public void evaluate(Channel[] inputChannels, Channel[] outputChannels) {

    }
}
