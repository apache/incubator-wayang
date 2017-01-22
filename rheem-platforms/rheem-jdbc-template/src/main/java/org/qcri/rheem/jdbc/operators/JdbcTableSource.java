package org.qcri.rheem.jdbc.operators;

import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.jdbc.compiler.FunctionCompiler;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("rheem.%s.tablesource.load", this.getPlatform().getPlatformId());
    }

    @Override
    public CardinalityEstimator getCardinalityEstimator(int outputIndex) {
        assert outputIndex == 0;
        return new CardinalityEstimator() {
            @Override
            public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
                // see Job for StopWatch measurements
                final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                        "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
                );

                // Establish a DB connection.
                try (Connection connection = JdbcTableSource.this.getPlatform()
                        .createDatabaseDescriptor(optimizationContext.getConfiguration())
                        .createJdbcConnection()) {

                    // Query the table cardinality.
                    final String sql = String.format("SELECT count(*) FROM %s;", JdbcTableSource.this.getTableName());
                    final ResultSet resultSet = connection.createStatement().executeQuery(sql);
                    if (!resultSet.next()) {
                        throw new SQLException("No query result for \"" + sql + "\".");
                    }
                    long cardinality = resultSet.getLong(1);
                    return new CardinalityEstimate(cardinality, cardinality, 1d);

                } catch (Exception e) {
                    LoggerFactory.getLogger(this.getClass()).error(
                            "Could not estimate cardinality for {}.", JdbcTableSource.this, e
                    );

                    // If we could not load the cardinality, let's use a very conservative estimate.
                    return new CardinalityEstimate(10, 10000000, 0.9);
                } finally {
                    timeMeasurement.stop();
                }
            }
        };
    }
}
