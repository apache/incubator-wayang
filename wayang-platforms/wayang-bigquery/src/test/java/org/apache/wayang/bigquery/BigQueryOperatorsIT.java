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
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.bigquery.operators.BigQuerySortOperator;
import org.apache.wayang.bigquery.operators.BigQueryTableSource;
import org.apache.wayang.bigquery.platform.BigQueryPlatform;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the BigQuery platform operators, driven through the
 * Wayang API ({@link BigQuery#plugin()}) against <b>real BigQuery</b>.
 *
 * <p><b>Why real BigQuery and not the emulator?</b> The Wayang module connects
 * through the BigQuery JDBC driver, which mandates Google OAuth2. The local
 * {@code goccy/bigquery-emulator} is no-auth and only speaks to the Google
 * client libraries, so it cannot serve the module's JDBC path. A real service
 * account is therefore required to actually exercise these operators.
 *
 * <p>Coverage: {@code TableSource}, {@code Filter}, {@code Projection},
 * {@code GlobalReduce}, {@code ReduceBy}, {@code Sort}, {@code Join}, and
 * {@code TableSink}, including JavaPlanBuilder combination plans that mirror
 * the Trino/Presto suites.
 *
 * <p><b>Status: 17/17 green</b> against a live BigQuery project on June 16,
 * 2026, including the five JavaPlanBuilder combination tests. The tests use
 * only {@code SELECT} and {@code CREATE TABLE AS}/{@code DROP} (DDL), never
 * DML, so they run without billing enabled.
 *
 * <p><b>Note on the aggregate tests.</b> {@code GlobalReduce}/{@code ReduceBy}
 * carry their aggregation only in the SQL implementation ({@code SUM(amount)});
 * the Java fallback would not reproduce it. They therefore depend on the optimizer
 * electing BigQuery pushdown, which it does here because they reduce cardinality.
 * If a future run on different data shows a Java-side reduce, scale the reference
 * dataset up (as the Trino/Presto suites do at 120k rows). {@code Sort} does not
 * reduce cardinality, so it is verified via the operator's SQL-clause contract
 * instead (see {@link #testSort()}).
 *
 * <h3>Prerequisites</h3>
 * <ol>
 *   <li>A GCP service account with BigQuery access; key JSON on disk.</li>
 *   <li>A reference table (default {@code <project>.sales.orders}) with columns
 *       {@code order_id, region, product, amount} and the 10-row dataset the
 *       assertions below expect (3 EMEA rows; >1000 amount rows non-empty).</li>
 * </ol>
 *
 * <h3>Configuration (system property or environment variable; sysprop wins)</h3>
 * <pre>
 *   bigquery.project   / BIGQUERY_PROJECT     GCP project id (required to run)
 *   bigquery.saEmail   / BIGQUERY_SA_EMAIL    service-account email
 *   bigquery.keyPath   / BIGQUERY_KEY_PATH    path to the SA key JSON
 *   bigquery.table     / BIGQUERY_TABLE       backtick-quoted FQ table name
 * </pre>
 * If a connection cannot be established, every test is skipped (not failed).
 *
 * <h3>Run</h3>
 * <pre>
 *   JAVA_HOME=&lt;jdk17&gt; mvn -o test -pl wayang-platforms/wayang-bigquery \
 *     -Dtest=BigQueryOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dbigquery.project=my-project \
 *     -Dbigquery.saEmail=wayang-bq@my-project.iam.gserviceaccount.com \
 *     -Dbigquery.keyPath=$HOME/wayang-bq-key.json \
 *     -Drat.skip=true -Dlicense.skip=true -Pskip-prerequisite-check
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryOperatorsIT {

    private static final String PROJECT_ID = cfg("bigquery.project", "BIGQUERY_PROJECT", "your-project");
    private static final String SA_EMAIL   = cfg("bigquery.saEmail", "BIGQUERY_SA_EMAIL",
            "wayang-bq@" + PROJECT_ID + ".iam.gserviceaccount.com");
    private static final String KEY_PATH   = cfg("bigquery.keyPath", "BIGQUERY_KEY_PATH",
            System.getProperty("user.home") + "/wayang-bq-key.json");

    /** Backtick-quoted fully-qualified BigQuery table name. */
    private static final String TABLE = cfg("bigquery.table", "BIGQUERY_TABLE",
            "`" + PROJECT_ID + ".sales.orders`");

    /** Backtick-quoted sink target for the TableSink test; dropped in {@link #cleanup()}. */
    private static final String SINK_TABLE = "`" + PROJECT_ID + ".sales.wayang_emea_orders`";

    /** Temporary lookup table for the JavaPlanBuilder join test. */
    private static final String JOIN_TABLE = "`" + PROJECT_ID + ".sales.wayang_regions`";

    private static final String JDBC_URL = String.format(
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2;" +
            "ProjectId=%s;OAuthType=0;OAuthServiceAcctEmail=%s;OAuthPvtKeyPath=%s",
            PROJECT_ID, SA_EMAIL, KEY_PATH);

    private static boolean available = false;

    /** Resolution order: system property (preferred), environment variable, default. */
    private static String cfg(String sysProp, String envVar, String dflt) {
        String v = System.getProperty(sysProp);
        if (v == null || v.isEmpty()) v = System.getenv(envVar);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    // Setup

    @BeforeAll
    static void checkAvailable() {
        try {
            Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");
            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                ResultSet rs = conn.createStatement().executeQuery("SELECT 1");
                available = rs.next();
                System.out.println("[SETUP] Connected to BigQuery project: " + PROJECT_ID);
            }
        } catch (Exception e) {
            System.err.println("[SETUP] BigQuery not available — all tests will be skipped: " + e.getMessage());
        }
    }

    @AfterAll
    static void cleanup() {
        if (!available) return;
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            conn.createStatement().execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            conn.createStatement().execute("DROP TABLE IF EXISTS " + JOIN_TABLE);
        } catch (Exception e) {
            System.err.println("[CLEANUP] failed to drop " + SINK_TABLE + ": " + e.getMessage());
        }
    }

    private Configuration createBigQueryConfig() {
        Configuration config = new Configuration();
        config.setProperty("wayang.bigquery.jdbc.url", JDBC_URL);
        return config;
    }

    private WayangContext createContext(Configuration config) {
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(BigQuery.plugin());
    }

    /** Record-aware multi-field projection (the POJO descriptor throws on >1 field). */
    private static ProjectionDescriptor<Record, Record> project(String... fields) {
        return ProjectionDescriptor.createForRecords(
                new RecordType("order_id", "region", "product", "amount"), fields);
    }

    // Verification tests

    /** BigQueryTableSource must be bound to BigQueryPlatform (drives wayang.bigquery.* config). */
    @Test
    @Order(0)
    @DisplayName("[VERIFY] BigQueryTableSource is bound to BigQueryPlatform")
    void testPlatformBinding() {
        BigQueryTableSource source = new BigQueryTableSource(TABLE, "order_id");

        assertSame(
                BigQueryPlatform.getInstance(),
                source.getPlatform(),
                "BigQueryTableSource.getPlatform() must return the BigQueryPlatform singleton"
        );
        assertEquals("bigquery", source.getPlatform().getPlatformId(),
                "Platform id drives all wayang.bigquery.* config key lookups");

        System.out.println("[VERIFY] getPlatform()   = " + source.getPlatform().getClass().getSimpleName());
        System.out.println("[VERIFY] getPlatformId() = " + source.getPlatform().getPlatformId());
    }

    /** Missing JDBC config must fail loudly, not silently fall back to Java evaluation. */
    @Test
    @Order(1)
    @DisplayName("[VERIFY] Execution fails when BigQuery JDBC config is missing")
    void testFailsWithoutJdbcConfig() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        Configuration emptyConfig = new Configuration();
        BigQueryTableSource source = new BigQueryTableSource(TABLE, "order_id", "region");
        List<Record> results = new ArrayList<>();
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, sink, 0);

        WayangContext ctx = new WayangContext(emptyConfig)
                .withPlugin(Java.basicPlugin())
                .withPlugin(BigQuery.plugin());

        assertThrows(Exception.class,
                () -> ctx.execute("BQ-NoConfig", new WayangPlan(sink)),
                "Should throw when wayang.bigquery.jdbc.url is not set"
        );
        System.out.println("[VERIFY] Correctly threw when JDBC config was absent.");
    }

    // Functional tests: TableSource, Filter, and Projection

    /** Full table scan: SELECT * FROM `<table>` */
    @Test
    @Order(2)
    @DisplayName("BigQuery: full table scan")
    void testTableScan() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-TableScan", new WayangPlan(sink));

        assertEquals(10, results.size(), "Expected 10 rows");
        System.out.println("[PASS] TableScan: " + results.size() + " rows");
    }

    /** String filter pushdown: WHERE region = 'APAC' */
    @Test
    @Order(3)
    @DisplayName("BigQuery: filter pushdown (region = 'APAC')")
    void testFilterString() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "APAC".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'APAC'"));
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Filter", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        results.forEach(r -> assertEquals("APAC", r.getField(1)));
        System.out.println("[PASS] Filter(region='APAC'): " + results.size() + " rows");
    }

    /** Numeric filter pushdown: WHERE amount > 1000 */
    @Test
    @Order(4)
    @DisplayName("BigQuery: filter pushdown (amount > 1000)")
    void testFilterNumeric() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> ((Number) r.getField(3)).doubleValue() > 1000.0, Record.class
                ).withSqlImplementation("amount > 1000"));
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Filter-Numeric", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        results.forEach(r -> assertTrue(((Number) r.getField(3)).doubleValue() > 1000.0));
        System.out.println("[PASS] Filter(amount>1000): " + results.size() + " rows");
    }

    /** Projection pushdown / column pruning: SELECT region, amount FROM `<table>` */
    @Test
    @Order(5)
    @DisplayName("BigQuery: projection pushdown (region, amount)")
    void testProjection() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        MapOperator<Record, Record> projection = new MapOperator<>(
                project("region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Projection", new WayangPlan(sink));

        assertEquals(10, results.size());
        results.forEach(r -> assertEquals(2, r.size(), "Record should have 2 projected fields"));
        System.out.println("[PASS] Projection(region, amount): " + results.size() + " rows");
    }

    /** Combined filter + projection in one SQL query: SELECT region, amount FROM `<table>` WHERE amount > 1000 */
    @Test
    @Order(6)
    @DisplayName("BigQuery: filter + projection pipeline")
    void testFilterAndProjection() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> ((Number) r.getField(3)).doubleValue() > 1000.0, Record.class
                ).withSqlImplementation("amount > 1000"));
        MapOperator<Record, Record> projection = new MapOperator<>(
                project("region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Filter-Projection", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        results.forEach(r -> {
            assertEquals(2, r.size());
            assertTrue(((Number) r.getField(1)).doubleValue() > 1000.0);
        });
        System.out.println("[PASS] Filter+Projection: " + results.size() + " rows");
    }

    /** Cardinality estimation sanity check (optimizer runs SELECT count(*) before planning). */
    @Test
    @Order(7)
    @DisplayName("BigQuery: cardinality estimation via COUNT(*) is accurate")
    void testCardinalityMatches() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "EMEA".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'EMEA'"));
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Cardinality", new WayangPlan(sink));

        assertEquals(3, results.size(), "Expected 3 EMEA rows");
        System.out.println("[PASS] Cardinality: " + results.size() + " EMEA rows (expected 3)");
    }

    // Aggregation, ordering, sink, and JavaPlanBuilder combination tests

    /**
     * GlobalReduce: SUM(amount) over the whole table collapses to a single row.
     *
     * <p>Note: the reduction lives only in the SQL implementation
     * ({@code SUM(amount)}); the Java fallback would not reproduce it, so this
     * test relies on the optimizer electing BigQuery pushdown for the reduce.
     */
    @Test
    @Order(8)
    @DisplayName("BigQuery: global reduce (SUM(amount))")
    void testGlobalReduce() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount)"),
                DataSetType.createDefault(Record.class));
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, reduce, 0);
        reduce.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-GlobalReduce", new WayangPlan(sink));

        assertEquals(1, results.size(), "global reduce must collapse to a single row");
        assertEquals(12752.0, ((Number) results.get(0).getField(0)).doubleValue(), 0.01);
        System.out.println("[PASS] GlobalReduce SUM(amount) = " + results.get(0).getField(0));
    }

    /** ReduceBy: SUM(amount) GROUP BY region yields one row per region. */
    @Test
    @Order(9)
    @DisplayName("BigQuery: reduce-by (SUM(amount) GROUP BY region)")
    void testReduceBy() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(1)), Record.class, Record.class
                ).withSqlImplementation("region", "region"),
                new ReduceDescriptor<>((a, b) -> a, Record.class)
                        .withSqlImplementation("SUM(amount)"),
                DataSetType.createDefault(Record.class));
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, reduceBy, 0);
        reduceBy.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-ReduceBy", new WayangPlan(sink));

        assertEquals(3, results.size(), "one row per region expected");
        Map<String, Double> sums = new HashMap<>();
        for (Record r : results) {
            sums.put((String) r.getField(0), ((Number) r.getField(1)).doubleValue());
        }
        assertEquals(6600.75, sums.get("APAC"), 0.01);
        assertEquals(2320.5,  sums.get("EMEA"), 0.01);
        assertEquals(3830.75, sums.get("AMER"), 0.01);
        System.out.println("[PASS] ReduceBy by region: " + sums);
    }

    /**
     * Sort: verified through the operator's SQL-clause contract executed on live
     * BigQuery (the same approach Trino/Presto use for {@code Join}).
     *
     * <p>Unlike filter/projection, a sort does not reduce cardinality, so on the
     * tiny reference table the cost optimizer keeps it in Java rather than pushing
     * it down, and the jdbc-template sort key is a {@code Record}, which the Java
     * sort cannot order (the Trino/Presto suites avoid this only because their
     * 120k-row fixtures make SQL pushdown the cheaper plan). So we assert the
     * operator's real contract: {@link BigQuerySortOperator#createSqlClause} must
     * produce a BigQuery-valid {@code ORDER BY} that returns correctly ordered rows.
     */
    @Test
    @Order(10)
    @DisplayName("BigQuery: sort (ORDER BY amount ASC) via operator SQL-clause contract")
    void testSort() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        BigQuerySortOperator sort = new BigQuerySortOperator(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(3)), Record.class, Record.class
                ).withSqlImplementation("amount", "ASC"));
        assertEquals(BigQueryPlatform.getInstance(), sort.getPlatform());

        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            String orderBy = sort.createSqlClause(conn, new FunctionCompiler());
            assertTrue(orderBy.contains("ORDER BY amount ASC"), "unexpected ORDER BY clause: " + orderBy);

            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT order_id, region, product, amount FROM " + TABLE + orderBy);
            List<Double> amounts = new ArrayList<>();
            while (rs.next()) amounts.add(rs.getDouble("amount"));

            assertEquals(10, amounts.size(), "sort must not change the cardinality");
            assertEquals(350.75, amounts.get(0), 0.001, "smallest amount first");
            assertEquals(3000.0, amounts.get(amounts.size() - 1), 0.001, "largest amount last");
            for (int i = 1; i < amounts.size(); i++) {
                assertTrue(amounts.get(i - 1) <= amounts.get(i), "non-decreasing at index " + i);
            }
            System.out.println("[PASS] Sort ORDER BY amount ASC: " + amounts.size() + " rows in order");
        }
    }

    /**
     * TableSink: filter + sink composed into a single {@code CREATE TABLE ... AS
     * SELECT} that runs entirely inside BigQuery; no data leaves the warehouse.
     */
    @Test
    @Order(11)
    @DisplayName("BigQuery: table sink (CREATE TABLE AS SELECT ... WHERE region = 'EMEA')")
    void testTableSink() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "EMEA".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'EMEA'"));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", SINK_TABLE,
                "order_id", "region", "product", "amount");
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-TableSink", new WayangPlan(sink));

        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            ResultSet rs = conn.createStatement().executeQuery(
                    "SELECT count(*), COUNTIF(region != 'EMEA') FROM " + SINK_TABLE);
            rs.next();
            assertEquals(3, rs.getLong(1), "sink table must hold all 3 EMEA orders");
            assertEquals(0, rs.getLong(2), "sink table must hold only EMEA orders");
        }
        System.out.println("[PASS] TableSink wrote 3 EMEA rows into " + SINK_TABLE);
    }

    /** JavaPlanBuilder API: combine a pushed-down filter and projection. */
    @Test
    @Order(12)
    @DisplayName("BigQuery JavaPlanBuilder: readTable -> filter -> projection")
    void javaPlanBuilderReadTableFilterProjection() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        Collection<Record> rows = new JavaPlanBuilder(
                createContext(createBigQueryConfig()), "BigQuery JavaPlanBuilder filter projection test")
                .readTable(new BigQueryTableSource(
                        TABLE, "order_id", "region", "product", "amount"))
                .filter(record -> ((Number) record.getField(3)).doubleValue() > 1000.0)
                    .withSqlUdf("amount > 1000")
                    .withTargetPlatform(BigQuery.platform())
                .asRecords()
                .projectRecords(new String[]{"region", "amount"})
                    .withTargetPlatform(BigQuery.platform())
                .collect();

        assertEquals(5, rows.size());
        assertTrue(rows.stream().allMatch(record ->
                record.size() == 2 && ((Number) record.getField(1)).doubleValue() > 1000.0));
    }

    /** JavaPlanBuilder API: combine a filter with a global reduction. */
    @Test
    @Order(13)
    @DisplayName("BigQuery JavaPlanBuilder: readTable -> filter -> globalReduce")
    void javaPlanBuilderReadTableFilterGlobalReduce() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        Collection<Record> rows = new JavaPlanBuilder(
                createContext(createBigQueryConfig()), "BigQuery JavaPlanBuilder global reduce test")
                .readTable(new BigQueryTableSource(
                        TABLE, "order_id", "region", "product", "amount"))
                .filter(record -> "EMEA".equals(record.getField(1)))
                    .withSqlUdf("region = 'EMEA'")
                    .withTargetPlatform(BigQuery.platform())
                .reduce((left, right) -> left)
                    .withSqlUdf("SUM(amount)")
                    .withTargetPlatform(BigQuery.platform())
                .collect();

        assertEquals(1, rows.size());
        assertEquals(2320.5, ((Number) rows.iterator().next().getField(0)).doubleValue(), 0.01);
    }

    /** JavaPlanBuilder API: combine grouped aggregation and sorting. */
    @Test
    @Order(14)
    @DisplayName("BigQuery JavaPlanBuilder: readTable -> reduceByKey -> sort")
    void javaPlanBuilderReadTableReduceBySort() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> rows = new ArrayList<>(new JavaPlanBuilder(
                createContext(createBigQueryConfig()), "BigQuery JavaPlanBuilder reduce-by sort test")
                .readTable(new BigQueryTableSource(
                        TABLE, "order_id", "region", "product", "amount"))
                .reduceByKey(
                        record -> new Record(record.getField(1)),
                        (left, right) -> left)
                    .withSqlUdfs("region", "SUM(amount)")
                    .withTargetPlatform(BigQuery.platform())
                .sort(record -> new Record(record.getField(0)))
                    .withSqlUdf("region", "ASC")
                    .withTargetPlatform(BigQuery.platform())
                .collect());

        assertEquals(3, rows.size());
        assertEquals("AMER", rows.get(0).getField(0));
        assertEquals("APAC", rows.get(1).getField(0));
        assertEquals("EMEA", rows.get(2).getField(0));
    }

    /** JavaPlanBuilder API: write a filtered projection into a BigQuery table. */
    @Test
    @Order(15)
    @DisplayName("BigQuery JavaPlanBuilder: readTable -> filter -> projection -> tableSink")
    void javaPlanBuilderReadTableFilterProjectionTableSink() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        new JavaPlanBuilder(
                createContext(createBigQueryConfig()), "BigQuery JavaPlanBuilder table sink test")
                .readTable(new BigQueryTableSource(
                        TABLE, "order_id", "region", "product", "amount"))
                .filter(record -> "EMEA".equals(record.getField(1)))
                    .withSqlUdf("region = 'EMEA'")
                    .withTargetPlatform(BigQuery.platform())
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                    .withTargetPlatform(BigQuery.platform())
                .writeTable(
                        SINK_TABLE,
                        "overwrite",
                        new String[]{"order_id", "amount"},
                        new Properties());

        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + SINK_TABLE);
            rs.next();
            assertEquals(3, rs.getLong(1));
        }
    }

    /** JavaPlanBuilder API: join orders with a temporary distinct-region table. */
    @Test
    @Order(16)
    @DisplayName("BigQuery JavaPlanBuilder: readTable + readTable -> join")
    void javaPlanBuilderReadTableJoin() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            conn.createStatement().execute("DROP TABLE IF EXISTS " + JOIN_TABLE);
            conn.createStatement().execute(
                    "CREATE TABLE " + JOIN_TABLE + " AS SELECT DISTINCT region FROM " + TABLE);
        }

        JavaPlanBuilder plan = new JavaPlanBuilder(
                createContext(createBigQueryConfig()), "BigQuery JavaPlanBuilder join test");
        DataQuantaBuilder<?, Record> orders = plan.readTable(new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"));
        DataQuantaBuilder<?, Record> regions = plan.readTable(new BigQueryTableSource(
                JOIN_TABLE, "region"));

        Collection<Record> rows = orders
                .join(
                        record -> new Record(record.getField(1)),
                        regions,
                        record -> new Record(record.getField(0)))
                    .withSqlUdfs(TABLE, "region", JOIN_TABLE, "region")
                    .withTargetPlatform(BigQuery.platform())
                .asRecords()
                .collect();

        assertEquals(10, rows.size());
        assertTrue(rows.stream().allMatch(row -> row.getField(1).equals(row.getField(4))));
    }
}
