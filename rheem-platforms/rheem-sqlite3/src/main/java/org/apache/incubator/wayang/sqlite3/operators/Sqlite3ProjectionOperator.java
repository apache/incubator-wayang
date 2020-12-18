package io.rheem.rheem.sqlite3.operators;

import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.basic.function.ProjectionDescriptor;
import io.rheem.rheem.basic.operators.MapOperator;
import io.rheem.rheem.jdbc.operators.JdbcProjectionOperator;
import io.rheem.rheem.sqlite3.platform.Sqlite3Platform;

/**
 * Implementation of the {@link JdbcProjectionOperator} for the {@link Sqlite3Platform}.
 */
public class Sqlite3ProjectionOperator extends JdbcProjectionOperator {

    public Sqlite3ProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    public Sqlite3ProjectionOperator(Class<Record> inputClass, Class<Record> outputClass, String... fieldNames) {
        super(fieldNames);
    }

    public Sqlite3ProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public Sqlite3Platform getPlatform() {
        return Sqlite3Platform.getInstance();
    }

}
