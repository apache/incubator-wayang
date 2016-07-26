package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;


/**
 * Template for JDBC-based {@link FilterOperator}.
 */
public abstract class JdbcFilterOperator<Type> extends FilterOperator<Type> implements JdbcExecutionOperator {

    public JdbcFilterOperator(PredicateDescriptor<Type> predicateDescriptor) {
        super(predicateDescriptor);
    }

    public JdbcFilterOperator(PredicateDescriptor<Type> predicateDescriptor, DataSetType<Type> type) {
        super(predicateDescriptor, type);
    }

    public JdbcFilterOperator(PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        super(predicateDescriptor, typeClass);
    }

    public JdbcFilterOperator(FilterOperator<Type> that) {
        super(that);
    }

    public JdbcFilterOperator(DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor) {
        super(type, predicateDescriptor);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return compiler.compile(this.predicateDescriptor);
    }
}
