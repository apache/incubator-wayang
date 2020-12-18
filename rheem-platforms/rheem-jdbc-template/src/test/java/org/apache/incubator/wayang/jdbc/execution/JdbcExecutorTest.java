package io.rheem.rheem.jdbc.execution;

import org.junit.Assert;
import org.junit.Test;
import io.rheem.rheem.basic.data.Record;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.api.Job;
import io.rheem.rheem.core.function.PredicateDescriptor;
import io.rheem.rheem.core.optimizer.DefaultOptimizationContext;
import io.rheem.rheem.core.plan.executionplan.ExecutionStage;
import io.rheem.rheem.core.plan.executionplan.ExecutionTask;
import io.rheem.rheem.core.platform.CrossPlatformExecutor;
import io.rheem.rheem.core.profiling.NoInstrumentationStrategy;
import io.rheem.rheem.jdbc.channels.SqlQueryChannel;
import io.rheem.rheem.jdbc.operators.JdbcFilterOperator;
import io.rheem.rheem.jdbc.operators.JdbcProjectionOperator;
import io.rheem.rheem.jdbc.operators.JdbcTableSource;
import io.rheem.rheem.jdbc.operators.SqlToStreamOperator;
import io.rheem.rheem.jdbc.test.HsqldbFilterOperator;
import io.rheem.rheem.jdbc.test.HsqldbPlatform;
import io.rheem.rheem.jdbc.test.HsqldbProjectionOperator;
import io.rheem.rheem.jdbc.test.HsqldbTableSource;

import java.sql.SQLException;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JdbcExecutor}.
 */
public class JdbcExecutorTest {

    @Test
    public void testExecuteWithPlainTableSource() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(tableSourceTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        tableSourceTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        Assert.assertEquals(
                "SELECT * FROM customer;",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }

    @Test
    public void testExecuteWithFilter() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcFilterOperator ageFilterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> {
                            throw new UnsupportedOperationException();
                        },
                        Record.class
                ).withSqlImplementation("age >= 18")
        );
        ExecutionTask ageFilterTask = new ExecutionTask(ageFilterOperator);
        ageFilterTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(ageFilterTask, 0);
        ageFilterTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, ageFilterOperator.getOutput(0)));

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(ageFilterTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        ageFilterTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        Assert.assertEquals(
                "SELECT * FROM customer WHERE age >= 18;",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }

    @Test
    public void testExecuteWithProjection() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcProjectionOperator projectionOperator = new HsqldbProjectionOperator("name", "age");
        ExecutionTask projectionTask = new ExecutionTask(projectionOperator);
        projectionTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(projectionTask, 0);
        projectionTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, projectionOperator.getOutput(0)));

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(projectionTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        projectionTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        Assert.assertEquals(
                "SELECT name, age FROM customer;",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }

    @Test
    public void testExecuteWithProjectionAndFilters() throws SQLException {
        Configuration configuration = new Configuration();
        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        JdbcTableSource tableSource = new HsqldbTableSource("customer");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        JdbcFilterOperator ageFilterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> {
                            throw new UnsupportedOperationException();
                        },
                        Record.class
                ).withSqlImplementation("age >= 18")
        );
        ExecutionTask ageFilterTask = new ExecutionTask(ageFilterOperator);
        ageFilterTask.setStage(sqlStage);
        tableSourceTask.getOutputChannel(0).addConsumer(ageFilterTask, 0);
        ageFilterTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, ageFilterOperator.getOutput(0)));

        JdbcFilterOperator nameFilterOperator = new HsqldbFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> {
                            throw new UnsupportedOperationException();
                        },
                        Record.class
                ).withSqlImplementation("name IS NOT NULL")
        );
        ExecutionTask nameFilterTask = new ExecutionTask(nameFilterOperator);
        nameFilterTask.setStage(sqlStage);
        ageFilterTask.getOutputChannel(0).addConsumer(nameFilterTask, 0);
        nameFilterTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, nameFilterOperator.getOutput(0)));

        JdbcProjectionOperator projectionOperator = new HsqldbProjectionOperator("name", "age");
        ExecutionTask projectionTask = new ExecutionTask(projectionOperator);
        projectionTask.setStage(sqlStage);
        nameFilterTask.getOutputChannel(0).addConsumer(projectionTask, 0);
        projectionTask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, projectionOperator.getOutput(0)));

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(projectionTask));

        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        projectionTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor().getChannelInstance(sqlToStreamTask.getInputChannel(0));
        Assert.assertEquals(
                "SELECT name, age FROM customer WHERE age >= 18 AND name IS NOT NULL;",
                sqlQueryChannelInstance.getSqlQuery()
        );
    }
}
