package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Optional;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public abstract class JdbcTableSource extends TableSource<Record> implements JdbcExecutionOperator {

    public JdbcTableSource(String tableName) {
        super(tableName, DataSetType.createDefault(Record.class));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JdbcTableSource(JdbcTableSource that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return this.getTableName();
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final String estimatorKey = String.format("rheem.%s.tablesource.load", this.getPlatform().getPlatformId());
        final NestableLoadProfileEstimator operatorEstimator = NestableLoadProfileEstimator.parseSpecification(
                configuration.getStringProperty(estimatorKey)
        );
        return Optional.of(operatorEstimator);
    }
}
