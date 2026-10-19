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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JavaTableSink}.
 */
class JavaTableSinkTest extends JavaExecutionOperatorTestBase {

    private static final String JDBC_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String DRIVER = "org.h2.Driver";
    private static final String TABLE_NAME = "test_table";

    private Connection connection;

    @BeforeEach
    void setupTest() throws Exception {
        Class.forName(DRIVER);
        connection = DriverManager.getConnection(JDBC_URL, "sa", "");
    }

    @AfterEach
    void teardownTest() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            }
            connection.close();
        }
    }

    @Test
    void testWritingRecordToH2() throws Exception {
        Configuration configuration = new Configuration();
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        JavaTableSink<Record> sink = new JavaTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "name", "value" },
                DataSetType.createDefault(Record.class));

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance inputChannelInstance = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);

        Record record1 = new Record(1, "Alice", 100.5);
        Record record2 = new Record(2, "Bob", 200.75);

        inputChannelInstance.accept(Stream.of(record1, record2));

        evaluate(sink, new ChannelInstance[] { inputChannelInstance }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + TABLE_NAME + "\"")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testWritingPojoToH2() throws Exception {
        Configuration configuration = new Configuration();
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        JavaTableSink<TestPojo> sink = new JavaTableSink<>(dbProps, "overwrite", TABLE_NAME,
                null,
                DataSetType.createDefault(TestPojo.class));

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance inputChannelInstance = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);

        TestPojo p1 = new TestPojo(1, "Alice");
        TestPojo p2 = new TestPojo(2, "Bob");

        inputChannelInstance.accept(Stream.of(p1, p2));

        evaluate(sink, new ChannelInstance[] { inputChannelInstance }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            rs.next();
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            rs.next();
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
        }
    }

    @Test
    void testAppendMode() throws Exception {
        Configuration configuration = new Configuration();
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        // 1. Initial write
        JavaTableSink<Record> sink1 = new JavaTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "name" },
                DataSetType.createDefault(Record.class));

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance input1 = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);
        input1.accept(Stream.of(new Record(1, "Alice")));
        evaluate(sink1, new ChannelInstance[] { input1 }, new ChannelInstance[0]);

        // 2. Append write
        JavaTableSink<Record> sink2 = new JavaTableSink<>(dbProps, "append", TABLE_NAME,
                new String[] { "id", "name" },
                DataSetType.createDefault(Record.class));

        Job job2 = mock(Job.class);
        when(job2.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor2 = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job2);

        StreamChannel.Instance input2 = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor2, mock(OptimizationContext.OperatorContext.class), 0);
        input2.accept(Stream.of(new Record(2, "Bob")));
        evaluate(sink2, new ChannelInstance[] { input2 }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + TABLE_NAME + "\"")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testOverwriteWithSchemaMismatch() throws Exception {
        Configuration configuration = new Configuration();
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        // 1. Create table with old schema
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT, name VARCHAR(255))");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Old')");
        }

        // 2. Overwrite with new schema
        JavaTableSink<Record> sink = new JavaTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "age", "city" },
                DataSetType.createDefault(Record.class));

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance input = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);
        input.accept(Stream.of(new Record(2, 30, "Berlin")));
        evaluate(sink, new ChannelInstance[] { input }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + TABLE_NAME + "\"")) {
            rs.next();
            assertEquals(2, rs.getInt("id"));
            assertEquals(30, rs.getInt("age"));
            assertEquals("Berlin", rs.getString("city"));

            // Verify 'name' column is gone
            boolean hasName = false;
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if ("name".equalsIgnoreCase(rs.getMetaData().getColumnName(i))) {
                    hasName = true;
                }
            }
            assertFalse(hasName, "Column 'name' should have been dropped");
        }
    }

    @Test
    void testNullValues() throws Exception {
        Configuration configuration = new Configuration();
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        JavaTableSink<Record> sink = new JavaTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "name" },
                DataSetType.createDefault(Record.class));

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance input = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);

        input.accept(Stream.of(new Record(1, null)));
        evaluate(sink, new ChannelInstance[] { input }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT \"name\" FROM \"" + TABLE_NAME + "\" WHERE \"id\" = 1")) {
            rs.next();
            assertEquals(null, rs.getString(1));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    void testSupportedTypes() throws Exception {
        Configuration configuration = new Configuration();
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        JavaTableSink<Record> sink = new JavaTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "is_active", "salary", "score" },
                DataSetType.createDefault(Record.class));

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance input = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);

        input.accept(Stream.of(new Record(1, true, 5000.50, 95.5f)));
        evaluate(sink, new ChannelInstance[] { input }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + TABLE_NAME + "\" WHERE \"id\" = 1")) {
            rs.next();
            assertTrue(rs.getBoolean("is_active"));
            assertEquals(5000.50, rs.getDouble("salary"), 0.001);
            assertEquals(95.5f, rs.getFloat("score"), 0.001f);
        }
    }

    @Test
    void testWritingAllSupportedTypesToDatabase() throws Exception {
        Configuration configuration = new Configuration();
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        JavaTableSink<Record> sink = new JavaTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "int_col", "long_col", "short_col", "double_col", "float_col",
                        "decimal_col", "bool_col", "string_col" },
                DataSetType.createDefault(Record.class));

        Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        final JavaExecutor javaExecutor = (JavaExecutor) JavaPlatform.getInstance().createExecutor(job);

        StreamChannel.Instance input = (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                .createChannel(mock(OutputSlot.class), configuration)
                .createInstance(javaExecutor, mock(OptimizationContext.OperatorContext.class), 0);

        BigDecimal decimalValue = new BigDecimal("12.345");
        input.accept(Stream.of(new Record(
                42, 9_000_000_000L, (short) 7, 3.14d, 1.5f, decimalValue, true, "hello")));
        evaluate(sink, new ChannelInstance[] { input }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + TABLE_NAME + "\"")) {
            rs.next();

            // values written through the Java sink are read back from the database unchanged
            assertEquals(42, rs.getInt("int_col"));
            assertEquals(9_000_000_000L, rs.getLong("long_col"));
            assertEquals((short) 7, rs.getShort("short_col"));
            assertEquals(3.14d, rs.getDouble("double_col"), 1e-9);
            assertEquals(1.5f, rs.getFloat("float_col"), 1e-6f);
            assertEquals(0, decimalValue.compareTo(rs.getBigDecimal("decimal_col")));
            assertTrue(rs.getBoolean("bool_col"));
            assertEquals("hello", rs.getString("string_col"));

            // the two new types were created with the right SQL column types
            ResultSetMetaData md = rs.getMetaData();

            int shortIdx = rs.findColumn("short_col");
            assertEquals(Types.SMALLINT, md.getColumnType(shortIdx), "Short -> SMALLINT");

            int decimalIdx = rs.findColumn("decimal_col");
            assertEquals(Types.NUMERIC, md.getColumnType(decimalIdx), "BigDecimal -> NUMERIC");
            assertEquals(38, md.getPrecision(decimalIdx), "BigDecimal precision must be 38");
            assertEquals(18, md.getScale(decimalIdx), "BigDecimal scale must be 18");
        }
    }

    public static class TestPojo {
        private int id;
        private String name;

        public TestPojo() {
        }

        public TestPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}