/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.test.HsqldbFilterOperator;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlToRddOperatorTest extends OperatorTestBase {

    @Test
    public void testWithHsqldb() throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);

        CrossPlatformExecutor cpe = new CrossPlatformExecutor(job, new FullInstrumentationStrategy());
        when(job.getCrossPlatformExecutor()).thenReturn(cpe);
        final SparkExecutor sparkExecutor = new SparkExecutor(SparkPlatform.getInstance(), job);

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testSqlToRddWithHsqldb (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testSqlToRddWithHsqldb VALUES (0, 'zero');");
            statement.execute("INSERT INTO testSqlToRddWithHsqldb VALUES (1, 'one');");
            statement.execute("INSERT INTO testSqlToRddWithHsqldb VALUES (2, 'two');");
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
        sqlQueryChannelInstance.setSqlQuery("SELECT * FROM testSqlToRddWithHsqldb;");
        ExecutionTask producer = new ExecutionTask(filterOperator);
        producer.setOutputChannel(0, sqlQueryChannel);

        RddChannel.Instance rddChannelInstance =
                new RddChannel(RddChannel.UNCACHED_DESCRIPTOR, mock(OutputSlot.class)).createInstance(
                        sparkExecutor,
                        mock(OptimizationContext.OperatorContext.class),
                        0
                );

        SqlToRddOperator sqlToRddOperator = new SqlToRddOperator(HsqldbPlatform.getInstance());
        evaluate(
                sqlToRddOperator,
                new ChannelInstance[]{sqlQueryChannelInstance},
                new ChannelInstance[]{rddChannelInstance}
        );

        List<Record> output = rddChannelInstance.<Record>provideRdd().collect();
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
        final SparkExecutor sparkExecutor = new SparkExecutor(SparkPlatform.getInstance(), job);

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testSqlToRddWithEmptyHsqldb (a INT, b VARCHAR(6));");
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
        sqlQueryChannelInstance.setSqlQuery("SELECT * FROM testSqlToRddWithEmptyHsqldb;");
        ExecutionTask producer = new ExecutionTask(filterOperator);
        producer.setOutputChannel(0, sqlQueryChannel);

        RddChannel.Instance rddChannelInstance =
                new RddChannel(RddChannel.UNCACHED_DESCRIPTOR, mock(OutputSlot.class)).createInstance(
                        sparkExecutor,
                        mock(OptimizationContext.OperatorContext.class),
                        0
                );

        SqlToRddOperator sqlToRddOperator = new SqlToRddOperator(HsqldbPlatform.getInstance());
        evaluate(
                sqlToRddOperator,
                new ChannelInstance[]{sqlQueryChannelInstance},
                new ChannelInstance[]{rddChannelInstance}
        );

        List<Record> output = rddChannelInstance.<Record>provideRdd().collect();
        Assert.assertTrue(output.isEmpty());
    }

}
