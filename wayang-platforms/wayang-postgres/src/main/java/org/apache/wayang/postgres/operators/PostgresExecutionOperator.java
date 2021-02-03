package org.apache.wayang.postgres.operators;

import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.wayang.postgres.platform.PostgresPlatform;

public interface PostgresExecutionOperator extends JdbcExecutionOperator {

    @Override
    default PostgresPlatform getPlatform() {
        return PostgresPlatform.getInstance();
    }

}
