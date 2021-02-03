package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

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
        return String.format("wayang.%s.projection.load", this.getPlatform().getPlatformId());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JdbcExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.functionDescriptor, configuration);
        return optEstimator;
    }

}
