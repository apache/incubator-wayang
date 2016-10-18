package org.qcri.rheem.sqlite3.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.jdbc.operators.JdbcProjectionOperator;
import org.qcri.rheem.sqlite3.platform.Sqlite3Platform;

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
