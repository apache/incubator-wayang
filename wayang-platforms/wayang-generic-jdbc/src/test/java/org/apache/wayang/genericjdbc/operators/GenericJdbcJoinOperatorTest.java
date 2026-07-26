package org.apache.wayang.genericjdbc.operators;

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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.JdbcExecutor;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GenericJdbcJoinOperatorTest {

    @Test
    void testJoinSqlGeneration() {

        Configuration configuration = new Configuration();
        
        configuration.setProperty("wayang.genericjdbc.jdbc.url", "jdbc:hsqldb:mem:wayang_test_db_filter;DB_CLOSE_DELAY=-1");
        configuration.setProperty("wayang.genericjdbc.jdbc.user", "SA");
        configuration.setProperty("wayang.genericjdbc.jdbc.password", "");
        configuration.setProperty("wayang.genericjdbc.jdbc.driverName", "org.hsqldb.jdbcDriver");

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor())
                .thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));

        GenericJdbcPlatform platform = GenericJdbcPlatform.getInstance();
        SqlQueryChannel.Descriptor sqlChannelDescriptor =
                platform.getGenericSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        GenericJdbcTableSource tableSourceA = new GenericJdbcTableSource("T1", "A", "VAL1");
        GenericJdbcTableSource tableSourceB = new GenericJdbcTableSource("T2", "A", "VAL2");

        ExecutionTask tableSourceATask = new ExecutionTask(tableSourceA);
        tableSourceATask.setOutputChannel(
                0,
                new SqlQueryChannel(sqlChannelDescriptor, tableSourceA.getOutput(0))
        );
        tableSourceATask.setStage(sqlStage);

        ExecutionTask tableSourceBTask = new ExecutionTask(tableSourceB);
        tableSourceBTask.setOutputChannel(
                0,
                new SqlQueryChannel(sqlChannelDescriptor, tableSourceB.getOutput(0))
        );
        tableSourceBTask.setStage(sqlStage);

        ExecutionOperator joinOperator = new GenericJdbcJoinOperator<Integer>(
                new TransformationDescriptor<Record, Integer>(
                        record -> (Integer) record.getField(0),
                        Record.class,
                        Integer.class
                ).withSqlImplementation("T1", "A"),
                new TransformationDescriptor<Record, Integer>(
                        record -> (Integer) record.getField(0),
                        Record.class,
                        Integer.class
                ).withSqlImplementation("T2", "A")
        );

        ExecutionTask joinTask = new ExecutionTask(joinOperator);

        tableSourceATask.getOutputChannel(0).addConsumer(joinTask, 0);
        tableSourceBTask.getOutputChannel(0).addConsumer(joinTask, 1);

        joinTask.setOutputChannel(
                0,
                new SqlQueryChannel(sqlChannelDescriptor, joinOperator.getOutput(0))
        );
        joinTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceATask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(joinTask));

        // Add SqlToStreamOperator stage (required by JdbcExecutor)
        ExecutionStage nextStage = mock(ExecutionStage.class);

        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(platform);
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);

        joinTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        JdbcExecutor executor = new JdbcExecutor(platform, job);

        executor.execute(
                sqlStage,
                new DefaultOptimizationContext(job),
                job.getCrossPlatformExecutor()
        );

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance)
                        job.getCrossPlatformExecutor()
                                .getChannelInstance(sqlToStreamTask.getInputChannel(0));

        String sql = sqlQueryChannelInstance.getSqlQuery();

        assertTrue(sql.contains("JOIN"));
        assertTrue(sql.contains("ON"));
    }
}