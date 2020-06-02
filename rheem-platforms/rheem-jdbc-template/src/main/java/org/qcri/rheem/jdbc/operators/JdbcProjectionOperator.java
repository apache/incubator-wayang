package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Optional;

/**
 * Projects the fields of {@link Record}s.
 */
public abstract class JdbcProjectionOperator extends MapOperator<Record, Record>
        implements JdbcExecutionOperator {

    public JdbcProjectionOperator(String... fieldNames) {
        super(
                new ProjectionDescriptor<>(Record.class, Record.class, fieldNames),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
    }

    public JdbcProjectionOperator(ProjectionDescriptor<Record, Record> functionDescriptor) {
        super(functionDescriptor);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcProjectionOperator(MapOperator<Record, Record> that) {
        super(that);
        if (!(that.getFunctionDescriptor() instanceof ProjectionDescriptor)) {
            throw new IllegalArgumentException("Can only copy from MapOperators with ProjectionDescriptors.");
        }
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return String.join(", ", this.getFunctionDescriptor().getFieldNames());
    }

    @Override
    public ProjectionDescriptor<Record, Record> getFunctionDescriptor() {
        return (ProjectionDescriptor<Record, Record>) super.getFunctionDescriptor();
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("rheem.%s.projection.load", this.getPlatform().getPlatformId());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JdbcExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
        return optEstimator;
    }

}
