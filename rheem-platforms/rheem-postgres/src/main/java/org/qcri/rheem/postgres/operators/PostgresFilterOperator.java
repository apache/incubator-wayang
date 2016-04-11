package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;

import java.util.function.Predicate;



/**
 * Postgres implementation of the {@link FilterOperator}.
 */
public class PostgresFilterOperator<Type>
        extends FilterOperator<Type>
        implements PostgresExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type type of the dataset elements
     */
    public PostgresFilterOperator(DataSetType<Type> type, PredicateDescriptor<Type> predicateDescriptor) {
        super(predicateDescriptor, type);
    }


    public PostgresFilterOperator(DataSetType<Type> type, PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor) {
        super(type, predicateDescriptor);
    }

    public PostgresFilterOperator(PredicateDescriptor.SerializablePredicate<Type> predicateDescriptor, Class<Type> typeClass) {
        super(predicateDescriptor, typeClass);
    }

    @Override
    @SuppressWarnings("unchecked")
    public String evaluate(Channel[] inputChannels, Channel[] outputChannels, FunctionCompiler compiler) {

        final String whereClause = compiler.compile(this.predicateDescriptor);
        return whereClause;
    }


    @Override
    protected ExecutionOperator createCopy() {
        return new PostgresFilterOperator<>(this.getInputType(), this.getPredicateDescriptor());
    }
}
