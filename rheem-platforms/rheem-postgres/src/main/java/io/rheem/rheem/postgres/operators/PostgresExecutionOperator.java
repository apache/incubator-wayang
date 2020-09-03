package io.rheem.rheem.postgres.operators;

import io.rheem.rheem.jdbc.operators.JdbcExecutionOperator;
import io.rheem.rheem.postgres.platform.PostgresPlatform;

public interface PostgresExecutionOperator extends JdbcExecutionOperator {

    @Override
    default PostgresPlatform getPlatform() {
        return PostgresPlatform.getInstance();
    }

}
