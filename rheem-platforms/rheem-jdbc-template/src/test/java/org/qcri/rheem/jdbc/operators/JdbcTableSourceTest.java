package org.qcri.rheem.jdbc.operators;

import de.hpi.isg.profiledb.instrumentation.StopWatch;
import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
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
        Job job = mock(Job.class);
        DefaultOptimizationContext optimizationContext = mock(DefaultOptimizationContext.class);
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
        when(optimizationContext.getJob()).thenReturn(job);
        when(job.getStopWatch()).thenReturn(new StopWatch(new Experiment("mock", new Subject("mock", "mock"))));
        when(optimizationContext.getConfiguration()).thenReturn(new Configuration());
        when(job.getConfiguration()).thenReturn(new Configuration());
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testCardinalityEstimator (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (0, 'zero');");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (1, 'one');");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (2, 'two');");
        }

        JdbcTableSource tableSource = new HsqldbTableSource("testCardinalityEstimator");
        final CardinalityEstimator cardinalityEstimator = tableSource.getCardinalityEstimator(0);

        final CardinalityEstimate estimate = cardinalityEstimator.estimate(optimizationContext);

        Assert.assertEquals(
                new CardinalityEstimate(3, 3, 1d),
                estimate
        );
    }

}
