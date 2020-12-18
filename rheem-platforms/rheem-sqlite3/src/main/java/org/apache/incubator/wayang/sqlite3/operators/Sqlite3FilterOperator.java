package io.rheem.rheem.sqlite3.operators;

import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.basic.operators.FilterOperator;
import io.rheem.rheem.core.function.PredicateDescriptor;
import io.rheem.rheem.jdbc.operators.JdbcFilterOperator;
import io.rheem.rheem.sqlite3.platform.Sqlite3Platform;

/**
 * Implementation of the {@link FilterOperator} for the {@link Sqlite3Platform}.
 */
public class Sqlite3FilterOperator extends JdbcFilterOperator {

    public Sqlite3FilterOperator(PredicateDescriptor<Record> predicateDescriptor) {
        super(predicateDescriptor);
    }

    public Sqlite3FilterOperator(FilterOperator<Record> that) {
        super(that);
    }

    @Override
    public Sqlite3Platform getPlatform() {
        return Sqlite3Platform.getInstance();
    }

}
