package org.apache.incubator.wayang.postgres.operators;

import org.apache.incubator.wayang.basic.data.Record;
import org.apache.incubator.wayang.basic.operators.FilterOperator;
import org.apache.incubator.wayang.core.function.PredicateDescriptor;
import org.apache.incubator.wayang.jdbc.operators.JdbcFilterOperator;


/**
 * PostgreSQL implementation of the {@link FilterOperator}.
 */
public class PostgresFilterOperator extends JdbcFilterOperator implements PostgresExecutionOperator {

    /**
     * Creates a new instance.
     */
    public PostgresFilterOperator(PredicateDescriptor<Record> predicateDescriptor) {
        super(predicateDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PostgresFilterOperator(FilterOperator<Record> that) {
        super(that);
    }

    @Override
    protected PostgresFilterOperator createCopy() {
        return new PostgresFilterOperator(this);
    }
}
