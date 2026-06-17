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

package org.apache.wayang.presto;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.presto.operators.PrestoTableSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests for every operator the Presto platform implements
 * (TableSource, Filter, Projection, Join, GlobalReduce, ReduceBy, Sort, TableSink)
 * driven through the Wayang API against a <b>live PrestoDB</b> cluster using the
 * in-memory connector.
 *
 * <p>Each operator runs through the full Wayang API (WayangContext to optimizer
 * to SQL-to-Stream) and asserts both correct results and that the expected SQL
 * was pushed down (via {@code system.runtime.queries}). Join uses a small
 * normalization map because the logical {@code JoinOperator} emits
 * {@code Tuple2<Record,Record>}, while a pushed-down JDBC join can return a flat
 * {@code Record}.
 *
 * <p>Prerequisites: a Presto reachable at {@code PRESTO_HOST:PRESTO_PORT}
 * (defaults {@code localhost:8081}) with the {@code memory} connector enabled;
 * e.g. {@code cd presto-setup && docker compose up -d}. If Presto is not
 * reachable the whole class is skipped (not failed).
 *
 * <pre>
 *   JAVA_HOME=&lt;jdk17&gt; mvn -o test -pl wayang-platforms/wayang-presto \
 *     -Dtest=PrestoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Drat.skip=true -Dlicense.skip=true -Pskip-prerequisite-check
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PrestoOperatorsIT {

    private static final String HOST = System.getenv().getOrDefault("PRESTO_HOST", "localhost");
    private static final int    PORT = Integer.parseInt(System.getenv().getOrDefault("PRESTO_PORT", "8081"));
    private static final String USER = System.getenv().getOrDefault("PRESTO_USER", "test");
    // Default catalog `memory`; tables are still referenced fully-qualified.
    private static final String JDBC_URL = String.format("jdbc:presto://%s:%d/memory", HOST, PORT);

    private static final String SCHEMA     = "memory.wayang_it";
    private static final String ORDERS     = SCHEMA + ".orders";
    private static final String CUSTOMERS  = SCHEMA + ".customers";
    private static final String SINK_TABLE = SCHEMA + ".amer_orders";

    private static boolean prestoAvailable = false;

    // Lifecycle

    @BeforeAll
    static void setUp() throws Exception {
        // Reachability probe: ONLY a genuine connection failure skips the class.
        // (A failure while building fixtures below is a real regression and must
        // surface as a test failure, not a silent skip.)
        try (Connection probe = jdbc()) {
            probe.createStatement().execute("SELECT 1");
            prestoAvailable = true;
        } catch (Exception e) {
            System.err.println("[PrestoOperatorsIT] Presto not reachable (" + e.getMessage() + ") — skipping.");
            return;
        }
        // Connected: build the fixtures. Exceptions here propagate and fail @BeforeAll.
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);

            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("CREATE TABLE " + ORDERS
                    + " (order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE)");
            // Build 120000 rows from a 6-row VALUES list crossed with sequence(1,10000)
            // and a 2-row doubler. Sourcing from VALUES (not the table itself) avoids
            // reading+writing the same memory table in one statement. Ratios preserved:
            // 3 of 6 base rows are AMER, giving 60000 AMER rows; customer_id is in {100,101,102}.
            st.execute("INSERT INTO " + ORDERS + " (order_id, customer_id, region, amount) "
                    + "SELECT CAST(b.order_id AS BIGINT), CAST(b.customer_id AS BIGINT), "
                    + "       b.region, CAST(b.amount AS DOUBLE) "
                    + "FROM UNNEST(sequence(1, 10000)) AS s(n) "
                    + "CROSS JOIN (VALUES (1), (2)) AS d(k) "
                    + "CROSS JOIN (VALUES "
                    + "  (1, 100, 'AMER', 2200.0),"
                    + "  (2, 101, 'EMEA',  800.5),"
                    + "  (3, 100, 'AMER',  680.5),"
                    + "  (4, 102, 'APAC', 1500.0),"
                    + "  (5, 101, 'EMEA', 1100.0),"
                    + "  (6, 100, 'AMER',  950.25)"
                    + ") AS b(order_id, customer_id, region, amount)");

            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("CREATE TABLE " + CUSTOMERS
                    + " (customer_id BIGINT, name VARCHAR, tier VARCHAR)");
            st.execute("INSERT INTO " + CUSTOMERS + " VALUES "
                    + "(CAST(100 AS BIGINT), 'Acme',    'GOLD'),"
                    + "(101, 'Globex',  'SILVER'),"
                    + "(102, 'Initech', 'BRONZE')");

        }
        System.out.println("[PrestoOperatorsIT] Connected to Presto at " + JDBC_URL + " — fixtures created.");
    }

    @AfterAll
    static void tearDown() {
        if (!prestoAvailable) return;
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
        } catch (Exception e) {
            System.err.println("[PrestoOperatorsIT] cleanup failed: " + e.getMessage());
        }
    }

    // Tests (one per operator)

    /** TableSource: full scan returns every row. */
    @Test
    @Order(1)
    void tableSource() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        List<Record> rows = execute(plan -> {
            PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(plan, Record.class);
            src.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(120000, rows.size(), "TableSource should return all orders");
        // @Order(1) runs this before the filter/join tests, whose pushed queries
        // contain this same prefix, so the (existential) history match here is the
        // bare scan, not one of those. @Order is therefore load-bearing for specificity.
        assertSqlReachedPresto("SELECT * FROM " + ORDERS);
    }

    /** Filter: WHERE region = 'AMER' pushed to Presto. */
    @Test
    @Order(2)
    void filter() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        List<Record> rows = execute(plan -> {
            PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            FilterOperator<Record> filter = new FilterOperator<>(
                    new PredicateDescriptor<>(
                            (Record r) -> "AMER".equals(r.getField(2)), Record.class
                    ).withSqlImplementation("region = 'AMER'"));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(plan, Record.class);
            src.connectTo(0, filter, 0);
            filter.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(60000, rows.size(), "60000 AMER orders expected");
        assertTrue(rows.stream().allMatch(r -> "AMER".equals(r.getField(2))), "all rows AMER");
        assertSqlReachedPresto("WHERE region = 'AMER'");
    }

    /** Projection (+filter): only the projected columns are fetched. */
    @Test
    @Order(3)
    void projection() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        List<Record> rows = execute(plan -> {
            PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            FilterOperator<Record> filter = new FilterOperator<>(
                    new PredicateDescriptor<>(
                            (Record r) -> "AMER".equals(r.getField(2)), Record.class
                    ).withSqlImplementation("region = 'AMER'"));
            MapOperator<Record, Record> projection = new MapOperator<>(
                    ProjectionDescriptor.createForRecords(
                            new RecordType("order_id", "customer_id", "region", "amount"),
                            "region", "amount"),
                    DataSetType.createDefault(Record.class),
                    DataSetType.createDefault(Record.class));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(plan, Record.class);
            src.connectTo(0, filter, 0);
            filter.connectTo(0, projection, 0);
            projection.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(60000, rows.size(), "60000 AMER rows expected");
        assertEquals(2, rows.get(0).size(), "projection keeps only 2 columns");
        assertSqlReachedPresto("SELECT region, amount FROM " + ORDERS);
    }

    /**
     * Join: orders with customers on customer_id.
     *
     * <p>The logical {@link JoinOperator} emits {@code Tuple2<Record,Record>},
     * while a pushed-down JDBC join already emits a flat {@link Record}. The
     * following map normalizes both representations before the result reaches
     * the sink.
     */
    @Test
    @Order(4)
    void join() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        List<Record> rows = execute(results -> {
            PrestoTableSource orders = new PrestoTableSource(
                    ORDERS, "order_id", "customer_id", "region", "amount");
            PrestoTableSource customers = new PrestoTableSource(
                    CUSTOMERS, "customer_id", "name", "tier");
            JoinOperator<Record, Record, Record> join = new JoinOperator<>(
                    new TransformationDescriptor<>(
                            (Record r) -> new Record(r.getField(1)), Record.class, Record.class
                    ).withSqlImplementation(ORDERS, "customer_id"),
                    new TransformationDescriptor<>(
                            (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                    ).withSqlImplementation(CUSTOMERS, "customer_id"));
            MapOperator<Object, Record> flatten = new MapOperator<>(
                    PrestoOperatorsIT::flattenJoinResult, Object.class, Record.class);
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
            orders.connectTo(0, join, 0);
            customers.connectTo(0, join, 1);
            join.connectTo(0, flatten, 0);
            flatten.connectTo(0, sink, 0);
            return sink;
        });

        assertEquals(120000, rows.size(), "join should yield one row per order");
        assertTrue(rows.stream().allMatch(row -> row.getField(1).equals(row.getField(4))),
                "joined customer IDs should match");
        assertSqlReachedPresto("JOIN " + CUSTOMERS);
    }

    /** GlobalReduce: SUM(amount) over the whole table collapses to a single row. */
    @Test
    @Order(5)
    void globalReduce() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        List<Record> rows = execute(plan -> {
            PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                    new ReduceDescriptor<>((a, b) -> a, Record.class)
                            .withSqlImplementation("SUM(amount)"),
                    DataSetType.createDefault(Record.class));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(plan, Record.class);
            src.connectTo(0, reduce, 0);
            reduce.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(1, rows.size(), "global reduce must collapse to a single row");
        // 6 base rows sum to 7231.25; scaled x20000 gives 144,625,000 (exact in doubles).
        assertEquals(144_625_000.0, ((Number) rows.get(0).getField(0)).doubleValue(), 0.01);
        assertSqlReachedPresto("SELECT SUM(amount) FROM " + ORDERS);
    }

    /** ReduceBy: SUM(amount) GROUP BY region yields one row per region. */
    @Test
    @Order(6)
    void reduceBy() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        List<Record> rows = execute(plan -> {
            PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                    new TransformationDescriptor<>(
                            (Record r) -> new Record(r.getField(2)), Record.class, Record.class
                    ).withSqlImplementation("region", "region"),
                    new ReduceDescriptor<>((a, b) -> a, Record.class)
                            .withSqlImplementation("SUM(amount)"),
                    DataSetType.createDefault(Record.class));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(plan, Record.class);
            src.connectTo(0, reduceBy, 0);
            reduceBy.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(3, rows.size(), "one row per region expected");
        Map<String, Double> sums = new HashMap<>();
        for (Record r : rows) {
            sums.put((String) r.getField(0), ((Number) r.getField(1)).doubleValue());
        }
        // Base sums (AMER 3830.75, EMEA 1900.5, APAC 1500.0) scaled x20000.
        assertEquals(76_615_000.0, sums.get("AMER"), 0.01);
        assertEquals(38_010_000.0, sums.get("EMEA"), 0.01);
        assertEquals(30_000_000.0, sums.get("APAC"), 0.01);
        assertSqlReachedPresto("GROUP BY region");
    }

    /** Sort: ORDER BY amount ASC pushed to Presto, order preserved to the sink. */
    @Test
    @Order(7)
    void sort() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");
        List<Record> rows = execute(plan -> {
            PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            SortOperator<Record, Record> sort = new SortOperator<>(
                    new TransformationDescriptor<>(
                            (Record r) -> new Record(r.getField(3)), Record.class, Record.class
                    ).withSqlImplementation("amount", "ASC"),
                    DataSetType.createDefault(Record.class));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(plan, Record.class);
            src.connectTo(0, sort, 0);
            sort.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(120000, rows.size(), "sort must not change the cardinality");
        assertEquals(680.5, ((Number) rows.get(0).getField(3)).doubleValue(), 0.001, "smallest amount first");
        assertEquals(2200.0, ((Number) rows.get(rows.size() - 1).getField(3)).doubleValue(), 0.001, "largest amount last");
        for (int i = 1; i < rows.size(); i++) {
            double prev = ((Number) rows.get(i - 1).getField(3)).doubleValue();
            double curr = ((Number) rows.get(i).getField(3)).doubleValue();
            assertTrue(prev <= curr, "rows must be non-decreasing by amount at index " + i);
        }
        assertSqlReachedPresto("ORDER BY amount ASC");
    }

    /**
     * TableSink: filter + sink composed into a single {@code CREATE TABLE ... AS
     * SELECT} that runs entirely inside Presto; no data leaves the database.
     */
    @Test
    @Order(8)
    void tableSink() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        PrestoTableSource src = new PrestoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record r) -> "AMER".equals(r.getField(2)), Record.class
                ).withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", SINK_TABLE,
                "order_id", "customer_id", "region", "amount");
        src.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayangContext().execute(new WayangPlan(sink));

        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT count(*), count_if(region <> 'AMER') FROM " + SINK_TABLE);
            rs.next();
            assertEquals(60000, rs.getLong(1), "sink table must hold all AMER orders");
            assertEquals(0, rs.getLong(2), "sink table must hold only AMER orders");
        }
        assertSqlReachedPresto("CREATE TABLE " + SINK_TABLE + " AS");
    }

    // JavaPlanBuilder combination tests

    /**
     * JavaPlanBuilder API: read a table, filter it, project two columns, and
     * collect the result through a complete high-level Wayang plan.
     */
    @Test
    @Order(9)
    void javaPlanBuilderReadTableFilterProjection() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        Collection<Record> rows = new JavaPlanBuilder(
                wayangContext(), "Presto JavaPlanBuilder readTable integration test")
                .readTable(new PrestoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .collect();

        assertEquals(60000, rows.size(), "60000 projected AMER orders expected");
        assertTrue(rows.stream().allMatch(record -> record.size() == 2),
                "projection should retain only order_id and amount");
        assertSqlReachedPresto(
                "SELECT order_id, amount FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /** JavaPlanBuilder API: combine a filter with a global reduction. */
    @Test
    @Order(10)
    void javaPlanBuilderReadTableFilterGlobalReduce() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        Collection<Record> rows = new JavaPlanBuilder(
                wayangContext(), "Presto JavaPlanBuilder global reduce integration test")
                .readTable(new PrestoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .reduce((left, right) -> left)
                    .withSqlUdf("SUM(amount)")
                .collect();

        assertEquals(1, rows.size(), "global reduction should return one row");
        assertEquals(76_615_000.0,
                ((Number) rows.iterator().next().getField(0)).doubleValue(), 0.01);
        assertSqlReachedPresto(
                "SELECT SUM(amount) FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /** JavaPlanBuilder API: group by region, aggregate each group, and sort it. */
    @Test
    @Order(11)
    void javaPlanBuilderReadTableReduceBySort() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        List<Record> rows = new ArrayList<>(new JavaPlanBuilder(
                wayangContext(), "Presto JavaPlanBuilder reduce-by and sort integration test")
                .readTable(new PrestoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .reduceByKey(
                        record -> new Record(record.getField(2)),
                        (left, right) -> left)
                    .withSqlUdfs("region", "SUM(amount)")
                .sort(record -> new Record(record.getField(0)))
                    .withSqlUdf("region", "ASC")
                .collect());

        assertEquals(3, rows.size(), "one row per region expected");
        assertEquals("AMER", rows.get(0).getField(0));
        assertEquals("APAC", rows.get(1).getField(0));
        assertEquals("EMEA", rows.get(2).getField(0));
        assertSqlReachedPresto(
                "SELECT region,SUM(amount) FROM " + ORDERS + " GROUP BY region ORDER BY region ASC");
    }

    /** JavaPlanBuilder API: compose a filtered projection into a table sink. */
    @Test
    @Order(12)
    void javaPlanBuilderReadTableFilterProjectionTableSink() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        new JavaPlanBuilder(wayangContext(), "Presto JavaPlanBuilder table sink integration test")
                .readTable(new PrestoTableSource(
                        ORDERS, "order_id", "customer_id", "region", "amount"))
                .filter(record -> "AMER".equals(record.getField(2)))
                    .withSqlUdf("region = 'AMER'")
                .asRecords()
                .projectRecords(new String[]{"order_id", "amount"})
                .writeTable(
                        SINK_TABLE,
                        "overwrite",
                        new String[]{"order_id", "amount"},
                        new Properties());

        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM " + SINK_TABLE);
            rs.next();
            assertEquals(60000, rs.getLong(1), "sink table should contain projected AMER orders");
        }
        assertSqlReachedPresto(
                "CREATE TABLE " + SINK_TABLE
                        + " AS SELECT order_id, amount FROM " + ORDERS + " WHERE region = 'AMER'");
    }

    /** JavaPlanBuilder API: join two tables and collect the pushed-down records. */
    @Test
    @Order(13)
    void javaPlanBuilderReadTableJoin() {
        Assumptions.assumeTrue(prestoAvailable, "Presto not reachable");

        JavaPlanBuilder plan = new JavaPlanBuilder(
                wayangContext(), "Presto JavaPlanBuilder join integration test");
        DataQuantaBuilder<?, Record> orders = plan.readTable(new PrestoTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount"));
        DataQuantaBuilder<?, Record> customers = plan.readTable(new PrestoTableSource(
                CUSTOMERS, "customer_id", "name", "tier"));

        Collection<Record> rows = orders
                .join(
                        record -> new Record(record.getField(1)),
                        customers,
                        record -> new Record(record.getField(0)))
                    .withSqlUdfs(ORDERS, "customer_id", CUSTOMERS, "customer_id")
                .asRecords()
                .collect();

        assertEquals(120000, rows.size(), "join should yield one row per order");
        assertTrue(rows.stream().allMatch(row -> row.getField(1).equals(row.getField(4))),
                "joined customer IDs should match");
        assertSqlReachedPresto("JOIN " + CUSTOMERS);
    }

    private interface PlanBuilder {
        LocalCallbackSink<Record> build(List<Record> resultCollector);
    }

    private WayangContext wayangContext() {
        Configuration config = new Configuration();
        config.setProperty("wayang.presto.jdbc.url", JDBC_URL);
        config.setProperty("wayang.presto.jdbc.user", USER);
        // No password: the Presto JDBC driver rejects an (even empty) password on
        // a non-SSL connection.
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Presto.plugin());
    }

    private List<Record> execute(PlanBuilder builder) {
        List<Record> results = new ArrayList<>();
        LocalCallbackSink<Record> sink = builder.build(results);
        wayangContext().execute(new WayangPlan(sink));
        return results;
    }

    private static Record flattenJoinResult(Object joinResult) {
        if (joinResult instanceof Record) {
            return (Record) joinResult;
        }
        Tuple2<?, ?> pair = (Tuple2<?, ?>) joinResult;
        Record left = (Record) pair.field0;
        Record right = (Record) pair.field1;
        return new Record(
                left.getField(0),
                left.getField(1),
                left.getField(2),
                left.getField(3),
                right.getField(0),
                right.getField(1),
                right.getField(2));
    }

    /** Assert that Presto actually ran a query containing the given fragment. */
    private void assertSqlReachedPresto(String fragment) {
        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT count(*) FROM system.runtime.queries "
                  + "WHERE state = 'FINISHED' "
                  + "AND query LIKE '%" + fragment.replace("'", "''") + "%' "
                  + "AND query NOT LIKE '%system.runtime%'");
            rs.next();
            assertTrue(rs.getLong(1) > 0, "Expected a Presto query containing: " + fragment);
        } catch (Exception e) {
            throw new RuntimeException("query-history check failed", e);
        }
    }

    private static Connection jdbc() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", USER);
        return DriverManager.getConnection(JDBC_URL, p);
    }
}
