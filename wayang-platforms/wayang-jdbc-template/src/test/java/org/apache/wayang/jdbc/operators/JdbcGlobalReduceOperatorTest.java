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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.JdbcExecutor;
import org.apache.wayang.jdbc.test.HsqldbGlobalReduceOperator;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.jdbc.test.HsqldbTableSource;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link SqlToStreamOperator}.
 */
class JdbcGlobalReduceOperatorTest extends OperatorTestBase {
    @Test
    void testWithHsqldb() throws SQLException {
        final Configuration configuration = new Configuration();

        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor())
                .thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        final SqlQueryChannel.Descriptor sqlChannelDescriptor = HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        final ExecutionStage sqlStage = mock(ExecutionStage.class);

        final JdbcTableSource tableSourceA = new HsqldbTableSource("testA");
        
        final ExecutionTask tableSourceATask = new ExecutionTask(tableSourceA);
        tableSourceATask.setOutputChannel(0, new SqlQueryChannel(sqlChannelDescriptor, tableSourceA.getOutput(0)));
        tableSourceATask.setStage(sqlStage);

        final ExecutionOperator globalReduceOperator = new HsqldbGlobalReduceOperator(
                new ReduceDescriptor<Record>(null, Record.class).withSqlImplementation("COUNT(*)"));

        final ExecutionTask globalReduceTask = new ExecutionTask(globalReduceOperator);
        tableSourceATask.getOutputChannel(0).addConsumer(globalReduceTask, 0);
        globalReduceTask.setOutputChannel(0,
                new SqlQueryChannel(sqlChannelDescriptor, globalReduceOperator.getOutput(0)));
        globalReduceTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceATask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(globalReduceTask));

        final ExecutionStage nextStage = mock(ExecutionStage.class);

        final SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        final ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        globalReduceTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        
        final HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS testA (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testA VALUES (0, 'zero');");
            statement.execute("CREATE TABLE IF NOT EXISTS testB (a INT, b INT);");
            statement.execute("INSERT INTO testB VALUES (0, 100);");
        }

        final JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        final SqlQueryChannel.Instance sqlQueryChannelInstance = (SqlQueryChannel.Instance) job.getCrossPlatformExecutor()
                .getChannelInstance(sqlToStreamTask.getInputChannel(0));

        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            final java.sql.ResultSet resultSet = statement.executeQuery(sqlQueryChannelInstance.getSqlQuery());
            resultSet.next();
            final int count = resultSet.getInt(1);

            assertTrue(count > 0);
        }

        assertEquals("SELECT COUNT(*) FROM testA;", sqlQueryChannelInstance.getSqlQuery());
    }
}
