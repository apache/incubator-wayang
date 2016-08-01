package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Optional;


/**
 * Template for JDBC-based {@link FilterOperator}.
 */
public abstract class JdbcFilterOperator extends FilterOperator<Record> implements JdbcExecutionOperator {

    public JdbcFilterOperator(PredicateDescriptor<Record> predicateDescriptor) {
        super(predicateDescriptor);
    }

    public JdbcFilterOperator(FilterOperator<Record> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return compiler.compile(this.predicateDescriptor);
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final String estimatorKey = String.format("rheem.%s.filter.load", this.getPlatform().getPlatformId());
        final NestableLoadProfileEstimator operatorEstimator = NestableLoadProfileEstimator.parseSpecification(
                configuration.getStringProperty(estimatorKey)
        );
        final LoadProfileEstimator udfEstimator = configuration
                .getFunctionLoadProfileEstimatorProvider()
                .provideFor(this.predicateDescriptor);
        operatorEstimator.nest(udfEstimator);
        return Optional.of(operatorEstimator);
    }
}
