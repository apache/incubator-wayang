package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.ProjectionOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Optional;

/**
 * PostgreSQL implementation for the {@link ProjectionOperator}.
 */
public abstract class JdbcProjectionOperator extends ProjectionOperator<Record, Record>
        implements JdbcExecutionOperator {


    public JdbcProjectionOperator(String... fieldNames) {
        super(Record.class, Record.class, fieldNames);
    }

    public JdbcProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcProjectionOperator(ProjectionOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return String.join(", ", this.getFunctionDescriptor().getFieldNames());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final String estimatorKey = String.format("rheem.%s.projection.load", this.getPlatform().getPlatformId());
        final NestableLoadProfileEstimator operatorEstimator = NestableLoadProfileEstimator.parseSpecification(
                configuration.getStringProperty(estimatorKey)
        );
        return Optional.of(operatorEstimator);
    }

}
