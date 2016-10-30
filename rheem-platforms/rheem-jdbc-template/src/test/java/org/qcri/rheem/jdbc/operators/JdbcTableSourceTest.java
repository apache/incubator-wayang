package org.qcri.rheem.jdbc.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.jdbc.test.HsqldbPlatform;
import org.qcri.rheem.jdbc.test.HsqldbTableSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link SqlToStreamOperator}.
 */
public class JdbcTableSourceTest {

    @Test
    public void testCardinalityEstimator() throws SQLException {
        Configuration configuration = new Configuration();
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testCardinalityEstimator (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (0, 'zero');");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (1, 'one');");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (2, 'two');");
        }

        JdbcTableSource tableSource = new HsqldbTableSource("testCardinalityEstimator");
        final CardinalityEstimator cardinalityEstimator = tableSource.getCardinalityEstimator(0);
        OptimizationContext optimizationContext = mock(OptimizationContext.class);
        when(optimizationContext.getConfiguration()).thenReturn(configuration);
        final CardinalityEstimate estimate = cardinalityEstimator.estimate(optimizationContext);

        Assert.assertEquals(
                new CardinalityEstimate(3, 3, 1d),
                estimate
        );
    }

}
