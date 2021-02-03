package org.apache.wayang.postgres.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;

/**
 * PostgreSQL implementation of the {@link FilterOperator}.
 */
public class PostgresProjectionOperator extends JdbcProjectionOperator implements PostgresExecutionOperator {

    public PostgresProjectionOperator(String... fieldNames) {
        super(fieldNames);
    }

    public PostgresProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public PostgresProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
    }

    @Override
    protected PostgresProjectionOperator createCopy() {
        return new PostgresProjectionOperator(this);
    }

}
