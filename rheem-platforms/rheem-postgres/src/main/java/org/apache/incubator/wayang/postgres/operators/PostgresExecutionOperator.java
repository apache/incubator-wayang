package org.apache.incubator.wayang.postgres.operators;

import org.apache.incubator.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.incubator.wayang.postgres.platform.PostgresPlatform;

public interface PostgresExecutionOperator extends JdbcExecutionOperator {

    @Override
    default PostgresPlatform getPlatform() {
        return PostgresPlatform.getInstance();
    }

}
