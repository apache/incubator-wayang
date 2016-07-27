package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;


/**
 * Template for JDBC-based {@link FilterOperator}.
 */
public abstract class JdbcFilterOperator extends FilterOperator<Record> implements JdbcExecutionOperator {

    public JdbcFilterOperator(PredicateDescriptor<Record> predicateDescriptor) {
        super(predicateDescriptor);
    }

    public JdbcFilterOperator(PredicateDescriptor<Record> predicateDescriptor, DataSetType<Record> Record) {
        super(predicateDescriptor, Record);
    }

    public JdbcFilterOperator(PredicateDescriptor.SerializablePredicate<Record> predicateDescriptor, Class<Record> RecordClass) {
        super(predicateDescriptor, RecordClass);
    }

    public JdbcFilterOperator(FilterOperator<Record> that) {
        super(that);
    }

    public JdbcFilterOperator(DataSetType<Record> Record, PredicateDescriptor.SerializablePredicate<Record> predicateDescriptor) {
        super(Record, predicateDescriptor);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return compiler.compile(this.predicateDescriptor);
    }
}
