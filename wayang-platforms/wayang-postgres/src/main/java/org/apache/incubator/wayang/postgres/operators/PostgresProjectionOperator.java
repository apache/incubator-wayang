package org.apache.incubator.wayang.postgres.operators;

import org.apache.incubator.wayang.basic.data.Record;
import org.apache.incubator.wayang.basic.function.ProjectionDescriptor;
import org.apache.incubator.wayang.basic.operators.FilterOperator;
import org.apache.incubator.wayang.basic.operators.MapOperator;
import org.apache.incubator.wayang.jdbc.operators.JdbcProjectionOperator;

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
