package org.qcri.rheem.jdbc.operators;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public abstract class JdbcTableSource extends TableSource implements JdbcExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @see TableSource#TableSource(String, String...)
     */
    public JdbcTableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
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
    public Optional<LoadProfileEstimator<ExecutionOperator>> createLoadProfileEstimator(Configuration configuration) {
        final String estimatorKey = String.format("rheem.%s.tablesource.load", this.getPlatform().getPlatformId());
        final NestableLoadProfileEstimator<ExecutionOperator> operatorEstimator = LoadProfileEstimators.createFromJuelSpecification(
                configuration.getStringProperty(estimatorKey)
        );
        return Optional.of(operatorEstimator);
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(int outputIndex) {
        assert outputIndex == 0;
        return new CardinalityEstimator() {
            @Override
            public CardinalityEstimate estimate(Configuration configuration, CardinalityEstimate... inputEstimates) {
                // Establish a DB connection.
                try (Connection connection = JdbcTableSource.this.getPlatform()
                        .createDatabaseDescriptor(configuration)
                        .createJdbcConnection()) {

                    // Query the table cardinality.
                    final String sql = String.format("SELECT count(*) FROM %s;", JdbcTableSource.this.getTableName());
                    final ResultSet resultSet = connection.createStatement().executeQuery(sql);
                    if (!resultSet.next()) {
                        throw new SQLException("No query result for \"" + sql + "\".");
                    }
                    long cardinality = resultSet.getLong(1);
                    return new CardinalityEstimate(cardinality, cardinality, 1d);

                } catch (SQLException e) {
                    LoggerFactory.getLogger(this.getClass()).error(
                            "Could not estimate cardinality for {}.", JdbcTableSource.this, e
                    );

                    // If we could not load the cardinality, let's use a very conservative estimate.
                    return new CardinalityEstimate(10, 10000000, 0.9);
                }
            }
        };
    }
}
