package io.rheem.rheem.jdbc.operators;

import org.junit.Assert;
import org.junit.Test;
import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.api.Job;
import io.rheem.rheem.core.function.PredicateDescriptor;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.executionplan.ExecutionTask;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.plan.rheemplan.OutputSlot;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.CrossPlatformExecutor;
import io.rheem.rheem.core.profiling.FullInstrumentationStrategy;
import io.rheem.rheem.java.channels.StreamChannel;
import io.rheem.rheem.java.execution.JavaExecutor;
import io.rheem.rheem.java.platform.JavaPlatform;
import io.rheem.rheem.jdbc.channels.SqlQueryChannel;
import io.rheem.rheem.jdbc.test.HsqldbFilterOperator;
import io.rheem.rheem.jdbc.test.HsqldbPlatform;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link SqlToStreamOperator}.
 */
public class SqlToStreamOperatorTest extends OperatorTestBase {

    @Test
    public void testWithHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        CrossPlatformExecutor cpe = new CrossPlatformExecutor(job, new FullInstrumentationStrategy());
        when(job.getCrossPlatformExecutor()).thenReturn(cpe);
        final JavaExecutor javaExecutor = new JavaExecutor(JavaPlatform.getInstance(), job);

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testWithHsqldb (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testWithHsqldb VALUES (0, 'zero');");
            statement.execute("INSERT INTO testWithHsqldb VALUES (1, 'one');");
            statement.execute("INSERT INTO testWithHsqldb VALUES (2, 'two');");
        }

        final ExecutionOperator filterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(x -> false, Record.class)
        );
        final SqlQueryChannel sqlQueryChannel = new SqlQueryChannel(
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor(),
                filterOperator.getOutput(0)
        );
        SqlQueryChannel.Instance sqlQueryChannelInstance = sqlQueryChannel.createInstance(
                hsqldbPlatform.createExecutor(job),
                mock(OptimizationContext.OperatorContext.class),
                0
        );
        sqlQueryChannelInstance.setSqlQuery("SELECT * FROM testWithHsqldb;");
        ExecutionTask producer = new ExecutionTask(filterOperator);
        producer.setOutputChannel(0, sqlQueryChannel);

        StreamChannel.Instance streamChannelInstance =
                new StreamChannel(StreamChannel.DESCRIPTOR, mock(OutputSlot.class)).createInstance(
                        javaExecutor,
                        mock(OptimizationContext.OperatorContext.class),
                        0
                );

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        evaluate(
                sqlToStreamOperator,
                new ChannelInstance[]{sqlQueryChannelInstance},
                new ChannelInstance[]{streamChannelInstance}
        );

        List<Record> output = streamChannelInstance.<Record>provideStream().collect(Collectors.toList());
        List<Record> expected = Arrays.asList(
                new Record(0, "zero"),
                new Record(1, "one"),
                new Record(2, "two")
        );

        Assert.assertEquals(expected, output);
    }

    @Test
    public void testWithEmptyHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        CrossPlatformExecutor cpe = new CrossPlatformExecutor(job, new FullInstrumentationStrategy());
        when(job.getCrossPlatformExecutor()).thenReturn(cpe);
        final JavaExecutor javaExecutor = new JavaExecutor(JavaPlatform.getInstance(), job);

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testWithEmptyHsqldb (a INT, b VARCHAR(6));");
        }

        final ExecutionOperator filterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(x -> false, Record.class)
        );
        final SqlQueryChannel sqlQueryChannel = new SqlQueryChannel(
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor(),
                filterOperator.getOutput(0)
        );
        SqlQueryChannel.Instance sqlQueryChannelInstance = sqlQueryChannel.createInstance(
                hsqldbPlatform.createExecutor(job),
                mock(OptimizationContext.OperatorContext.class),
                0
        );
        sqlQueryChannelInstance.setSqlQuery("SELECT * FROM testWithEmptyHsqldb;");
        ExecutionTask producer = new ExecutionTask(filterOperator);
        producer.setOutputChannel(0, sqlQueryChannel);

        StreamChannel.Instance streamChannelInstance =
                new StreamChannel(StreamChannel.DESCRIPTOR, mock(OutputSlot.class)).createInstance(
                        javaExecutor,
                        mock(OptimizationContext.OperatorContext.class),
                        0
                );

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        evaluate(
                sqlToStreamOperator,
                new ChannelInstance[]{sqlQueryChannelInstance},
                new ChannelInstance[]{streamChannelInstance}
        );

        List<Record> output = streamChannelInstance.<Record>provideStream().collect(Collectors.toList());
        Assert.assertTrue(output.isEmpty());
    }

}
