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

package org.apache.wayang.spatial.operators.jdbc;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.NoInstrumentationStrategy;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.execution.JdbcExecutor;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.spatial.test.HsqldbPlatform;
import org.apache.wayang.spatial.test.HsqldbTableSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JdbcSpatialFilterOperator}.
 * Verifies that the generated SQL contains the expected spatial predicate clause.
 */
class JdbcSpatialFilterOperatorTest {

    /**
     * Concrete subclass for testing against HSQLDB.
     */
    private static class TestJdbcSpatialFilterOperator<Type> extends JdbcSpatialFilterOperator<Type> {

        TestJdbcSpatialFilterOperator(SpatialPredicate relation,
                                      FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry> keyExtractor,
                                      DataSetType<Type> inputClassDatasetType,
                                      SpatialGeometry geometry) {
            super(relation, keyExtractor, inputClassDatasetType, geometry);
        }

        @Override
        public JdbcPlatformTemplate getPlatform() {
            return HsqldbPlatform.getInstance();
        }
    }

    @Test
    void testSpatialFilterIntersectsGeneratesCorrectSql() throws SQLException {
        String sql = buildSpatialFilterSql(
                SpatialPredicate.INTERSECTS,
                new WayangGeometry("POINT (0 0)")
        );

        assertTrue(sql.startsWith("SELECT"),
                "SQL should be a SELECT statement, but was: " + sql);
        assertTrue(sql.contains("FROM testGeom"),
                "SQL should select from testGeom, but was: " + sql);
        assertTrue(sql.contains("ST_Intersects(geom, ST_GeomFromText('POINT (0 0)', 4326))"),
                "SQL should contain ST_Intersects predicate, but was: " + sql);
    }

    @Test
    void testSpatialFilterWithinGeneratesCorrectSql() throws SQLException {
        WayangGeometry polygon = new WayangGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        String sql = buildSpatialFilterSql(SpatialPredicate.WITHIN, polygon);

        assertTrue(sql.contains("ST_Within(geom, ST_GeomFromText('" + polygon.getWKT() + "', 4326))"),
                "SQL should contain ST_Within predicate, but was: " + sql);
    }

    @Test
    void testSpatialFilterContainsGeneratesCorrectSql() throws SQLException {
        WayangGeometry polygon = new WayangGeometry("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        String sql = buildSpatialFilterSql(SpatialPredicate.CONTAINS, polygon);

        assertTrue(sql.contains("ST_Contains(geom, ST_GeomFromText('" + polygon.getWKT() + "', 4326))"),
                "SQL should contain ST_Contains predicate, but was: " + sql);
    }

    /**
     * Sets up a JDBC execution pipeline (table source -> spatial filter -> SqlToStream)
     * and returns the generated SQL query string.
     */
    private String buildSpatialFilterSql(SpatialPredicate predicateType,
                                         WayangGeometry referenceGeometry) throws SQLException {
        Configuration configuration = new Configuration();

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor())
                .thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));

        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();
        SqlQueryChannel.Descriptor sqlChannelDescriptor =
                HsqldbPlatform.getInstance().getSqlQueryChannelDescriptor();

        ExecutionStage sqlStage = mock(ExecutionStage.class);

        // Create a simple test table with a "geom" column.
        try (Connection jdbcConnection =
                     hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS testGeom (id INT, geom VARCHAR(255));");
        }

        // Table source for testGeom.
        JdbcTableSource tableSource = new HsqldbTableSource("testGeom");
        ExecutionTask tableSourceTask = new ExecutionTask(tableSource);
        tableSourceTask.setOutputChannel(0,
                new SqlQueryChannel(sqlChannelDescriptor, tableSource.getOutput(0)));
        tableSourceTask.setStage(sqlStage);

        // Spatial filter operator with SQL implementation on the key descriptor.
        TestJdbcSpatialFilterOperator<Record> filterOp = new TestJdbcSpatialFilterOperator<>(
                predicateType,
                record -> WayangGeometry.fromStringInput((String) record.getField(1)),
                DataSetType.createDefault(Record.class),
                referenceGeometry
        );
        filterOp.getKeyDescriptor().withSqlImplementation("testGeom", "geom");

        ExecutionTask filterTask = new ExecutionTask(filterOp);
        tableSourceTask.getOutputChannel(0).addConsumer(filterTask, 0);
        filterTask.setOutputChannel(0,
                new SqlQueryChannel(sqlChannelDescriptor, filterOp.getOutput(0)));
        filterTask.setStage(sqlStage);

        when(sqlStage.getStartTasks()).thenReturn(Collections.singleton(tableSourceTask));
        when(sqlStage.getTerminalTasks()).thenReturn(Collections.singleton(filterTask));

        // Next stage that consumes the SQL.
        ExecutionStage nextStage = mock(ExecutionStage.class);
        SqlToStreamOperator sqlToStreamOperator = new SqlToStreamOperator(HsqldbPlatform.getInstance());
        ExecutionTask sqlToStreamTask = new ExecutionTask(sqlToStreamOperator);
        filterTask.getOutputChannel(0).addConsumer(sqlToStreamTask, 0);
        sqlToStreamTask.setStage(nextStage);

        // Execute the SQL stage to build the SQL string.
        JdbcExecutor executor = new JdbcExecutor(HsqldbPlatform.getInstance(), job);
        executor.execute(sqlStage, new DefaultOptimizationContext(job), job.getCrossPlatformExecutor());

        SqlQueryChannel.Instance sqlQueryChannelInstance =
                (SqlQueryChannel.Instance) job.getCrossPlatformExecutor()
                        .getChannelInstance(sqlToStreamTask.getInputChannel(0));

        return sqlQueryChannelInstance.getSqlQuery();
    }
}
