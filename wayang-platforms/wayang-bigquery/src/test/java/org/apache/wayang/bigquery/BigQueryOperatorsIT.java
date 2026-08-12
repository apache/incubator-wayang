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

package org.apache.wayang.bigquery;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.bigquery.operators.BigQueryProjectionOperator;
import org.apache.wayang.bigquery.operators.BigQueryTableSource;
import org.apache.wayang.bigquery.platform.BigQueryPlatform;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Engine-only end-to-end integration tests for every operator the BigQuery
 * platform implements, driven through the Wayang API against <b>real BigQuery</b>.
 *
 * <p>Coverage: {@code TableSource}, {@code Filter}, {@code Projection},
 * {@code Join}, {@code GlobalReduce}, {@code ReduceBy}, {@code Sort},
 * {@code TableSink}. Every Wayang plan ends in a BigQuery {@code TableSink} that
 * compiles to {@code CREATE TABLE `proj.ds.t` AS SELECT ...} executed inside
 * BigQuery. Only {@code BigQuery.plugin()} is registered; there is no
 * {@code Java.basicPlugin()}, so the optimizer has no Java operators to fall back
 * to and the whole plan necessarily runs in BigQuery. Assertions re-query the
 * sink table via plain JDBC only after {@code execute(...)} returns; the sink
 * table's existence + contents prove the CTAS ran in BigQuery.
 *
 * <p>The source tables are created from inline literals in {@link #setUp()}, so
 * no external BigQuery dataset or table is required. Requires a live GCP project
 * + service account (the JDBC driver mandates OAuth2; the local emulator cannot
 * serve it).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryOperatorsIT {

    private static final String PROJECT_ID = cfg("bigquery.project", "BIGQUERY_PROJECT", "your-project");
    private static final String SA_EMAIL   = cfg("bigquery.saEmail", "BIGQUERY_SA_EMAIL",
            "wayang-bq@" + PROJECT_ID + ".iam.gserviceaccount.com");
    private static final String KEY_PATH   = cfg("bigquery.keyPath", "BIGQUERY_KEY_PATH",
            System.getProperty("user.home") + "/wayang-bq-key.json");
    private static final String LOCATION = cfg("bigquery.location", "BIGQUERY_LOCATION", "US");
    private static final String DATASET = cfg("bigquery.dataset", "BIGQUERY_DATASET", "wayang_it");

    private static final String TABLE = tableName("orders");
    private static final String SINK_TABLE = tableName("operator_result");
    private static final String JOIN_TABLE = tableName("regions");
    private static final String[] JOIN_COLUMNS = {"order_id", "region", "product", "amount", "region_name"};
    private static final String JOIN_FLATTEN_NAME = "BigQuery test-only join flatten";

    private static final String JDBC_URL = String.format(
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2;" +
            "ProjectId=%s;OAuthType=0;OAuthServiceAcctEmail=%s;OAuthPvtKeyPath=%s;Location=%s",
            PROJECT_ID, SA_EMAIL, KEY_PATH, LOCATION);

    private static boolean available = false;

    private static String cfg(String sysProp, String envVar, String dflt) {
        String v = System.getProperty(sysProp);
        if (v == null || v.isEmpty()) v = System.getenv(envVar);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    private static String tableName(String table) {
        return "`" + PROJECT_ID + "." + DATASET + "." + table + "`";
    }

    private static void createFixtureTables(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS `" + PROJECT_ID + "." + DATASET + "` "
                    + "OPTIONS(location='" + LOCATION + "')");
            st.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            st.execute("DROP TABLE IF EXISTS " + JOIN_TABLE);
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE + " AS "
                    + "SELECT * FROM UNNEST(["
                    + "STRUCT(1 AS order_id, 'APAC' AS region, 'Widget A' AS product, 1500.0 AS amount),"
                    + "STRUCT(2 AS order_id, 'EMEA' AS region, 'Widget B' AS product, 800.5 AS amount),"
                    + "STRUCT(3 AS order_id, 'AMER' AS region, 'Widget A' AS product, 2200.0 AS amount),"
                    + "STRUCT(4 AS order_id, 'APAC' AS region, 'Widget C' AS product, 350.75 AS amount),"
                    + "STRUCT(5 AS order_id, 'EMEA' AS region, 'Widget A' AS product, 1100.0 AS amount),"
                    + "STRUCT(6 AS order_id, 'AMER' AS region, 'Widget B' AS product, 950.25 AS amount),"
                    + "STRUCT(7 AS order_id, 'APAC' AS region, 'Widget B' AS product, 1750.0 AS amount),"
                    + "STRUCT(8 AS order_id, 'EMEA' AS region, 'Widget C' AS product, 420.0 AS amount),"
                    + "STRUCT(9 AS order_id, 'AMER' AS region, 'Widget C' AS product, 680.5 AS amount),"
                    + "STRUCT(10 AS order_id, 'APAC' AS region, 'Widget A' AS product, 3000.0 AS amount)"
                    + "])");
            // Lookup table for the join tests; region_name avoids duplicate
            // region columns in the flattened CREATE TABLE AS SELECT.
            st.execute("CREATE TABLE " + JOIN_TABLE
                    + " AS SELECT DISTINCT region AS region_name FROM " + TABLE);
        }
    }

    // Lifecycle

    @BeforeAll
    static void setUp() {
        try {
            Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");
            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                ResultSet rs = conn.createStatement().executeQuery("SELECT 1");
                available = rs.next();
                createFixtureTables(conn);
                System.out.println("[SETUP] Connected to BigQuery project: " + PROJECT_ID);
            }
        } catch (Exception e) {
            System.err.println("[SETUP] BigQuery not available; all tests will be skipped: " + e.getMessage());
        }
    }

    @AfterAll
    static void cleanup() {
        if (!available) return;
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            conn.createStatement().execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            conn.createStatement().execute("DROP TABLE IF EXISTS " + JOIN_TABLE);
            conn.createStatement().execute("DROP TABLE IF EXISTS " + TABLE);
        } catch (Exception e) {
            System.err.println("[CLEANUP] failed: " + e.getMessage());
        }
    }

    // Tests (one per operator)

    @Test
    @Order(1)
    @DisplayName("BigQuery engine-only: TableSource -> TableSink")
    void tableSource() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource src = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        TableSink<Record> sink = tableSink("order_id", "region", "product", "amount");
        src.connectTo(0, sink, 0);

        wayangContext().execute("BQ-TableSource", new WayangPlan(sink));

        assertEquals(10, queryLong("SELECT count(*) FROM " + SINK_TABLE), "all 10 orders expected");
    }

    @Test
    @Order(2)
    @DisplayName("BigQuery engine-only: Filter -> TableSink")
    void filter() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource src = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "EMEA".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'EMEA'"));
        TableSink<Record> sink = tableSink("order_id", "region", "product", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute("BQ-Filter", new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE), "3 EMEA orders expected");
        assertEquals(0, queryLong("SELECT COUNTIF(region != 'EMEA') FROM " + SINK_TABLE), "only EMEA rows");
    }

    @Test
    @Order(3)
    @DisplayName("BigQuery engine-only: Projection -> TableSink")
    void projection() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource src = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "EMEA".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'EMEA'"));
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "region", "product", "amount"),
                        "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);

        wayangContext().execute("BQ-Projection", new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE), "3 EMEA rows expected");
        assertEquals(2, columnCount(SINK_TABLE), "projection keeps only 2 columns");
    }

    @Test
    @Order(4)
    @DisplayName("BigQuery engine-only: Join -> TableSink")
    void join() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource orders = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        BigQueryTableSource regions = new BigQueryTableSource(JOIN_TABLE, "region_name");
        JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(1)), Record.class, Record.class
                ).withSqlImplementation(TABLE, "region"),
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                ).withSqlImplementation(JOIN_TABLE, "region_name"));
        MapOperator<Tuple2<Record, Record>, Record> flatten = joinFlattenOperator();
        TableSink<Record> sink = tableSink(JOIN_COLUMNS);
        orders.connectTo(0, join, 0);
        regions.connectTo(0, join, 1);
        join.connectTo(0, flatten, 0);
        flatten.connectTo(0, sink, 0);

        wayangContext().execute("BQ-Join", new WayangPlan(sink));

        assertEquals(10, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "join yields one row per order (every region exists)");
        assertEquals(0, queryLong("SELECT COUNTIF(region != region_name) FROM " + SINK_TABLE),
                "joined regions should match");
    }

    @Test
    @Order(5)
    @DisplayName("BigQuery engine-only: GlobalReduce -> TableSink")
    void globalReduce() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource src = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("total_amount");
        src.connectTo(0, reduce, 0);
        reduce.connectTo(0, sink, 0);

        wayangContext().execute("BQ-GlobalReduce", new WayangPlan(sink));

        assertSingleDoubleResult(12752.0, "global reduce collapses to a single SUM row");
    }

    @Test
    @Order(6)
    @DisplayName("BigQuery engine-only: ReduceBy -> TableSink")
    void reduceBy() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource src = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(1)), Record.class, Record.class
                ).withSqlImplementation("region", "region"),
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount) AS total_amount"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("region", "total_amount");
        src.connectTo(0, reduceBy, 0);
        reduceBy.connectTo(0, sink, 0);

        wayangContext().execute("BQ-ReduceBy", new WayangPlan(sink));

        Map<String, Double> sums = readRegionSums();
        assertEquals(3, sums.size(), "one row per region expected");
        assertEquals(3830.75, sums.get("AMER"), 0.01);
        assertEquals(2320.5, sums.get("EMEA"), 0.01);
        assertEquals(6600.75, sums.get("APAC"), 0.01);
    }

    @Test
    @Order(7)
    @DisplayName("BigQuery engine-only: Sort -> TableSink")
    void sort() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource src = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        SortOperator<Record, Record> sort = new SortOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(3)), Record.class, Record.class
                ).withSqlImplementation("amount", "ASC"),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = tableSink("order_id", "region", "product", "amount");
        src.connectTo(0, sort, 0);
        sort.connectTo(0, sink, 0);

        wayangContext().execute("BQ-Sort", new WayangPlan(sink));

        assertEquals(10, queryLong("SELECT count(*) FROM " + SINK_TABLE), "sort preserves cardinality");
        assertEquals(350.75, queryDouble("SELECT min(amount) FROM " + SINK_TABLE), 0.001);
        assertEquals(3000.0, queryDouble("SELECT max(amount) FROM " + SINK_TABLE), 0.001);
    }

    @Test
    @Order(8)
    @DisplayName("BigQuery engine-only: TableSink (filter -> CREATE TABLE AS SELECT)")
    void tableSink() {
        Assumptions.assumeTrue(available, "BigQuery not available");
        BigQueryTableSource src = new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "EMEA".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'EMEA'"));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", SINK_TABLE,
                "order_id", "region", "product", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute("BQ-TableSink", new WayangPlan(sink));

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE), "sink holds all 3 EMEA orders");
        assertEquals(0, queryLong("SELECT COUNTIF(region != 'EMEA') FROM " + SINK_TABLE), "only EMEA rows");
    }

    // JavaPlanBuilder combination tests

    @Test
    @Order(9)
    @DisplayName("BigQuery engine-only JavaPlanBuilder: readTable -> filter -> projection -> tableSink")
    void javaPlanBuilderReadTableFilterProjection() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        new JavaPlanBuilder(wayangContext(), "BigQuery JavaPlanBuilder filter projection test")
                .readTable(new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount"))
                .filter(record -> "EMEA".equals(record.getField(1)))
                    .withSqlUdf("region = 'EMEA'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(SINK_TABLE, "overwrite", new String[]{"order_id", "amount"}, new Properties());

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE), "3 projected EMEA orders expected");
        assertEquals(2, columnCount(SINK_TABLE), "projection keeps only 2 columns");
    }

    @Test
    @Order(10)
    @DisplayName("BigQuery engine-only JavaPlanBuilder: readTable -> filter -> globalReduce -> tableSink")
    void javaPlanBuilderReadTableFilterGlobalReduce() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        new JavaPlanBuilder(wayangContext(), "BigQuery JavaPlanBuilder global reduce test")
                .readTable(new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount"))
                .filter(record -> "EMEA".equals(record.getField(1)))
                    .withSqlUdf("region = 'EMEA'")
                .reduce((left, right) -> left)
                    .withSqlUdf("SUM(amount) AS total_amount")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"total_amount"}, new Properties());

        assertSingleDoubleResult(2320.5, "global reduction over EMEA should return one row");
    }

    @Test
    @Order(11)
    @DisplayName("BigQuery engine-only JavaPlanBuilder: readTable -> reduceByKey -> sort -> tableSink")
    void javaPlanBuilderReadTableReduceBySort() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        new JavaPlanBuilder(wayangContext(), "BigQuery JavaPlanBuilder reduce-by sort test")
                .readTable(new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount"))
                .reduceByKey(
                        record -> new Record(record.getField(1)),
                        (left, right) -> left)
                    .withSqlUdfs("region", "SUM(amount) AS total_amount")
                .sort(record -> new Record(record.getField(0)))
                    .withSqlUdf("region", "ASC")
                .writeTable(SINK_TABLE, "overwrite", new String[]{"region", "total_amount"}, new Properties());

        Map<String, Double> sums = readRegionSums();
        assertEquals(3, sums.size(), "one row per region expected");
        assertTrue(sums.containsKey("AMER") && sums.containsKey("APAC") && sums.containsKey("EMEA"));
    }

    @Test
    @Order(12)
    @DisplayName("BigQuery engine-only JavaPlanBuilder: readTable -> filter -> projection -> writeTable")
    void javaPlanBuilderReadTableFilterProjectionTableSink() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        new JavaPlanBuilder(wayangContext(), "BigQuery JavaPlanBuilder table sink test")
                .readTable(new BigQueryTableSource(TABLE, "order_id", "region", "product", "amount"))
                .filter(record -> "EMEA".equals(record.getField(1)))
                    .withSqlUdf("region = 'EMEA'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(SINK_TABLE, "overwrite", new String[]{"order_id", "amount"}, new Properties());

        assertEquals(3, queryLong("SELECT count(*) FROM " + SINK_TABLE), "sink holds 3 projected EMEA orders");
    }

    @Test
    @Order(13)
    @DisplayName("BigQuery engine-only JavaPlanBuilder: readTable + readTable -> join -> tableSink")
    void javaPlanBuilderReadTableJoin() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        JavaPlanBuilder plan = new JavaPlanBuilder(wayangContext(), "BigQuery JavaPlanBuilder join test");
        DataQuantaBuilder<?, Record> orders = plan.readTable(new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"));
        DataQuantaBuilder<?, Record> regions = plan.readTable(new BigQueryTableSource(
                JOIN_TABLE, "region_name"));

        orders
                .join(
                        record -> new Record(record.getField(1)),
                        regions,
                        record -> new Record(record.getField(0)))
                    .withSqlUdfs(TABLE, "region", JOIN_TABLE, "region_name")
                .map(new JoinFlattenFunction())
                    .withName(JOIN_FLATTEN_NAME)
                .writeTable(SINK_TABLE, "overwrite", JOIN_COLUMNS, new Properties());

        assertEquals(10, queryLong("SELECT count(*) FROM " + SINK_TABLE),
                "join yields one row per order");
        assertEquals(0, queryLong("SELECT COUNTIF(region != region_name) FROM " + SINK_TABLE),
                "joined regions should match");
    }

    // Helpers

    private WayangContext wayangContext() {
        Configuration config = new Configuration();
        config.setProperty("wayang.bigquery.jdbc.url", JDBC_URL);
        config.getMappingProvider().addAllToWhitelist(
                Collections.singleton(new JoinFlattenMapping()));
        return new WayangContext(config)
                .withPlugin(BigQuery.plugin());
    }

    private TableSink<Record> tableSink(String... columnNames) {
        return new TableSink<>(new Properties(), "overwrite", SINK_TABLE, columnNames);
    }

    private static MapOperator<Tuple2<Record, Record>, Record> joinFlattenOperator() {
        MapOperator<Tuple2<Record, Record>, Record> operator = new MapOperator<>(
                new TransformationDescriptor<>(
                        new JoinFlattenFunction(),
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(Record.class)),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefault(Record.class));
        operator.setName(JOIN_FLATTEN_NAME);
        return operator;
    }

    private static Record flattenJoinResult(Object joinResult) {
        if (joinResult instanceof Record) {
            return (Record) joinResult;
        }
        Tuple2<?, ?> pair = (Tuple2<?, ?>) joinResult;
        Record left = (Record) pair.field0;
        Record right = (Record) pair.field1;
        return new Record(
                left.getField(0), left.getField(1), left.getField(2), left.getField(3),
                right.getField(0));
    }

    private long queryLong(String sql) {
        try (Connection c = DriverManager.getConnection(JDBC_URL); ResultSet rs = c.createStatement().executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private double queryDouble(String sql) {
        try (Connection c = DriverManager.getConnection(JDBC_URL); ResultSet rs = c.createStatement().executeQuery(sql)) {
            rs.next();
            return rs.getDouble(1);
        } catch (Exception e) {
            throw new RuntimeException("query failed: " + sql, e);
        }
    }

    private int columnCount(String table) {
        try (Connection c = DriverManager.getConnection(JDBC_URL);
                ResultSet rs = c.createStatement().executeQuery("SELECT * FROM " + table + " LIMIT 1")) {
            return rs.getMetaData().getColumnCount();
        } catch (Exception e) {
            throw new RuntimeException("query failed: column count of " + table, e);
        }
    }

    private void assertSingleDoubleResult(double expected, String message) {
        try (Connection c = DriverManager.getConnection(JDBC_URL);
                ResultSet rs = c.createStatement().executeQuery("SELECT * FROM " + SINK_TABLE)) {
            assertTrue(rs.next(), message);
            assertEquals(expected, rs.getDouble(1), 0.01, message);
            assertFalse(rs.next(), message);
        } catch (Exception e) {
            throw new RuntimeException("query failed: SELECT * FROM " + SINK_TABLE, e);
        }
    }

    private Map<String, Double> readRegionSums() {
        Map<String, Double> sums = new HashMap<>();
        try (Connection c = DriverManager.getConnection(JDBC_URL);
                ResultSet rs = c.createStatement().executeQuery("SELECT * FROM " + SINK_TABLE)) {
            while (rs.next()) {
                sums.put(rs.getString(1), rs.getDouble(2));
            }
            return sums;
        } catch (Exception e) {
            throw new RuntimeException("query failed: SELECT * FROM " + SINK_TABLE, e);
        }
    }

    private static final class JoinFlattenFunction implements
            FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Record> {
        @Override
        public Record apply(Tuple2<Record, Record> tuple) {
            return flattenJoinResult(tuple);
        }
    }

    /** Test-only mapping for the unresolved logical join Tuple-to-Record mismatch. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static final class JoinFlattenMapping implements Mapping {
        @Override
        public java.util.Collection<PlanTransformation> getTransformations() {
            OperatorPattern<MapOperator> pattern = new OperatorPattern(
                    "joinFlatten",
                    new MapOperator(null, DataSetType.none(), DataSetType.createDefault(Record.class)),
                    false)
                    .withAdditionalTest(operator -> JOIN_FLATTEN_NAME.equals(((MapOperator) operator).getName()));

            ReplacementSubplanFactory factory = new ReplacementSubplanFactory.OfSingleOperators<MapOperator>(
                    (matchedOperator, epoch) -> createBigQueryProjection().at(epoch));

            return Collections.singleton(new PlanTransformation(
                    SubplanPattern.createSingleton(pattern),
                    factory,
                    BigQueryPlatform.getInstance()));
        }

        private static BigQueryProjectionOperator createBigQueryProjection() {
            ProjectionDescriptor<Tuple2<Record, Record>, Record> descriptor = new ProjectionDescriptor<>(
                    new JoinFlattenFunction(),
                    Arrays.asList(JOIN_COLUMNS),
                    DataUnitType.createBasicUnchecked(Tuple2.class),
                    DataUnitType.createBasic(Record.class));
            MapOperator<Tuple2<Record, Record>, Record> projection = new MapOperator<>(
                    descriptor,
                    DataSetType.createDefaultUnchecked(Tuple2.class),
                    DataSetType.createDefault(Record.class));
            projection.setName(JOIN_FLATTEN_NAME);
            return new BigQueryProjectionOperator((MapOperator<Record, Record>) (MapOperator) projection);
        }
    }
}
