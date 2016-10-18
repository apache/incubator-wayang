package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;


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
