package org.apache.wayang.sqlite3.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;

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
