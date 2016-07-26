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

    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public JdbcFilterOperator(DataSetType<Type> type, PredicateDescriptor<Type> predicateDescriptor) {
        super(predicateDescriptor, type);
    }


    public JdbcFilterOperator(DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor) {
        super(type, predicateDescriptor);
    }

    public JdbcFilterOperator(PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        super(predicateDescriptor, typeClass);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcFilterOperator(FilterOperator<Type> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return compiler.compile(this.predicateDescriptor);
    }
}
