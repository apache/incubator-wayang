package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.jdbc.operators.JdbcExecutionOperator;
import org.qcri.rheem.postgres.platform.PostgresPlatform;

public interface PostgresExecutionOperator extends JdbcExecutionOperator {

    @Override
    default PostgresPlatform getPlatform() {
        return PostgresPlatform.getInstance();
    }

}