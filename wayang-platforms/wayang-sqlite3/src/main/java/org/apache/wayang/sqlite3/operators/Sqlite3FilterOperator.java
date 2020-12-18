package org.apache.incubator.wayang.sqlite3.operators;

import org.apache.incubator.wayang.basic.data.Record;
import org.apache.incubator.wayang.basic.operators.FilterOperator;
import org.apache.incubator.wayang.core.function.PredicateDescriptor;
import org.apache.incubator.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.incubator.wayang.sqlite3.platform.Sqlite3Platform;

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
