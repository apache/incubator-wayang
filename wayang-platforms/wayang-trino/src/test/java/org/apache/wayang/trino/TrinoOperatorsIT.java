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

package org.apache.wayang.trino;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
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
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.trino.operators.TrinoJoinOperator;
import org.apache.wayang.trino.operators.TrinoTableSource;
import org.apache.wayang.trino.platform.TrinoPlatform;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests for every operator the Trino platform implements,
 * driven through the Wayang API against a <b>live Trino</b> cluster.
 *
 * <p>Coverage: {@code TableSource}, {@code Filter}, {@code Projection},
 * {@code Join}, {@code GlobalReduce}, {@code ReduceBy}, {@code Sort},
 * {@code TableSink} — plus the SQL→Stream channel conversion that materialises
 * every result. Each test also asserts, via Trino's {@code system.runtime.queries},
 * that the expected SQL actually reached Trino (i.e. the operator was pushed
 * down, not silently executed elsewhere).
 *
 * <p>Prerequisites: a Trino reachable at {@code TRINO_HOST:TRINO_PORT}
 * (defaults {@code localhost:8080}); e.g. {@code cd trino-setup && docker compose up -d}.
 * If Trino is not reachable the whole class is skipped (not failed).
 *
 * <p>Run:
 * <pre>
 *   JAVA_HOME=&lt;jdk17&gt; mvn -o test -pl wayang-platforms/wayang-trino \
 *     -Dtest=TrinoOperatorsIT -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Drat.skip=true -Dlicense.skip=true -Pskip-prerequisite-check
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TrinoOperatorsIT {

    private static final String HOST = System.getenv().getOrDefault("TRINO_HOST", "localhost");
    private static final int    PORT = Integer.parseInt(System.getenv().getOrDefault("TRINO_PORT", "8080"));
    private static final String USER = System.getenv().getOrDefault("TRINO_USER", "admin");
    private static final String JDBC_URL = String.format("jdbc:trino://%s:%d", HOST, PORT);

    // Dedicated schema so the test is self-contained and side-effect free.
    private static final String SCHEMA     = "iceberg.wayang_it";
    private static final String ORDERS     = SCHEMA + ".orders";
    private static final String CUSTOMERS  = SCHEMA + ".customers";
    private static final String SINK_TABLE = SCHEMA + ".amer_orders";

    private static boolean trinoAvailable = false;

    // ── Lifecycle ───────────────────────────────────────────────────────────

    @BeforeAll
    static void setUp() {
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);

            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("CREATE TABLE " + ORDERS + " ("
                    + "order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE)"
                    + " WITH (format = 'PARQUET')");
            st.execute("INSERT INTO " + ORDERS + " VALUES "
                    + "(1, 100, 'AMER', 2200.0),"
                    + "(2, 101, 'EMEA',  800.5),"
                    + "(3, 100, 'AMER',  680.5),"
                    + "(4, 102, 'APAC', 1500.0),"
                    + "(5, 101, 'EMEA', 1100.0),"
                    + "(6, 100, 'AMER',  950.25)");
            // Scale up so Wayang's cost optimizer actually elects SQL pushdown
            // (on a tiny table it prefers a full scan + Java-side ops, which
            // makes the query-history assertions fail). Trino caps sequence()
            // at 10000 entries, so scale in two steps: x10000 then x2 = 120000
            // rows total; ratios preserved (AMER = 60000).
            st.execute("INSERT INTO " + ORDERS
                    + " SELECT order_id + (n * 10), customer_id, region, amount"
                    + " FROM " + ORDERS + ", UNNEST(sequence(1, 9999)) AS t(n)");   // 6 -> 60000
            st.execute("INSERT INTO " + ORDERS
                    + " SELECT order_id + 600000, customer_id, region, amount"
                    + " FROM " + ORDERS);                                            // 60000 -> 120000

            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("CREATE TABLE " + CUSTOMERS + " ("
                    + "customer_id BIGINT, name VARCHAR, tier VARCHAR)"
                    + " WITH (format = 'PARQUET')");
            st.execute("INSERT INTO " + CUSTOMERS + " VALUES "
                    + "(100, 'Acme',   'GOLD'),"
                    + "(101, 'Globex', 'SILVER'),"
                    + "(102, 'Initech','BRONZE')");

            trinoAvailable = true;
            System.out.println("[TrinoOperatorsIT] Connected to Trino at " + JDBC_URL + " — fixtures created.");
        } catch (Exception e) {
            System.err.println("[TrinoOperatorsIT] Trino not available (" + e.getMessage() + ") — skipping.");
        }
    }

    @AfterAll
    static void tearDown() {
        if (!trinoAvailable) return;
        try (Connection c = jdbc()) {
            Statement st = c.createStatement();
            st.execute("DROP TABLE IF EXISTS " + ORDERS);
            st.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
            st.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
        } catch (Exception e) {
            System.err.println("[TrinoOperatorsIT] cleanup failed: " + e.getMessage());
        }
    }

    // ── Tests (one per operator) ──────────────────────────────────────────────

    /** TableSource: full scan returns every row. */
    @Test
    @Order(1)
    void tableSource() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        List<Record> rows = execute(results -> {
            TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
            src.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(120000, rows.size(), "TableSource should return all orders");
        assertSqlReachedTrino("SELECT * FROM " + ORDERS);
    }

    /** Filter: WHERE region = 'AMER' pushed to Trino. */
    @Test
    @Order(2)
    void filter() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        List<Record> rows = execute(results -> {
            TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            FilterOperator<Record> filter = new FilterOperator<>(
                    new PredicateDescriptor<>(
                            (Record r) -> "AMER".equals(r.getField(2)), Record.class
                    ).withSqlImplementation("region = 'AMER'"));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
            src.connectTo(0, filter, 0);
            filter.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(60000, rows.size(), "60000 AMER orders expected");
        assertTrue(rows.stream().allMatch(r -> "AMER".equals(r.getField(2))), "all rows AMER");
        assertSqlReachedTrino("WHERE region = 'AMER'");
    }

    /** Projection (+filter): only the projected columns are fetched. */
    @Test
    @Order(3)
    void projection() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        List<Record> rows = execute(results -> {
            TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
            src.connectTo(0, filter, 0);
            filter.connectTo(0, projection, 0);
            projection.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(60000, rows.size(), "60000 AMER rows expected");
        assertEquals(2, rows.get(0).size(), "projection keeps only 2 columns");
        assertSqlReachedTrino("SELECT region, amount FROM " + ORDERS);
    }

    /**
     * Join: orders ⋈ customers on customer_id.
     *
     * <p>Unlike the other operators, a JDBC join cannot be driven through the
     * high-level {@link WayangContext} API here: the logical {@link JoinOperator}
     * emits {@code Tuple2<Record,Record>}, which has no valid connection to a
     * {@code Record} sink (the SQL pushdown that would flatten it happens only
     * after optimization). So we verify the operator's real contract directly:
     * {@link TrinoJoinOperator#createSqlClause} must produce a Trino-valid JOIN
     * clause that, executed against live Trino, returns the correct rows.
     */
    @Test
    @Order(4)
    void join() throws Exception {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        TrinoJoinOperator<Record> join = new TrinoJoinOperator<>(
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(1)), Record.class, Record.class
                ).withSqlImplementation(ORDERS, "customer_id"),
                new TransformationDescriptor<>(
                        (Record r) -> new Record(r.getField(0)), Record.class, Record.class
                ).withSqlImplementation(CUSTOMERS, "customer_id"));

        // The operator routes to the Trino platform.
        assertEquals(TrinoPlatform.getInstance(), join.getPlatform());

        try (Connection c = jdbc()) {
            // Operator-generated JOIN clause, assembled into the full SELECT exactly
            // as JdbcExecutor would (FROM <left> + <join clause>).
            String joinClause = join.createSqlClause(c, new FunctionCompiler());
            assertTrue(joinClause.startsWith("JOIN " + CUSTOMERS + " ON"),
                    "unexpected join clause: " + joinClause);

            String sql = "SELECT * FROM " + ORDERS + " " + joinClause;
            ResultSet rs = c.createStatement().executeQuery(sql);
            int n = 0;
            while (rs.next()) n++;
            // Every order's customer_id exists in customers → one joined row per order.
            assertEquals(120000, n, "join should yield one row per order");
        }
    }

    /** GlobalReduce: SUM(amount) over the whole table collapses to a single row. */
    @Test
    @Order(5)
    void globalReduce() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        List<Record> rows = execute(results -> {
            TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            GlobalReduceOperator<Record> reduce = new GlobalReduceOperator<>(
                    new ReduceDescriptor<>((a, b) -> a, Record.class)
                            .withSqlImplementation("SUM(amount)"),
                    DataSetType.createDefault(Record.class));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
            src.connectTo(0, reduce, 0);
            reduce.connectTo(0, sink, 0);
            return sink;
        });
        assertEquals(1, rows.size(), "global reduce must collapse to a single row");
        // 6 base rows sum to 7231.25; scaled x20000 → 144,625,000 (exact in doubles).
        assertEquals(144_625_000.0, ((Number) rows.get(0).getField(0)).doubleValue(), 0.01);
        assertSqlReachedTrino("SELECT SUM(amount) FROM " + ORDERS);
    }

    /** ReduceBy: SUM(amount) GROUP BY region yields one row per region. */
    @Test
    @Order(6)
    void reduceBy() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        List<Record> rows = execute(results -> {
            TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            ReduceByOperator<Record, Record> reduceBy = new ReduceByOperator<>(
                    new TransformationDescriptor<>(
                            (Record r) -> new Record(r.getField(2)), Record.class, Record.class
                    ).withSqlImplementation("region", "region"),
                    new ReduceDescriptor<>((a, b) -> a, Record.class)
                            .withSqlImplementation("SUM(amount)"),
                    DataSetType.createDefault(Record.class));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
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
        assertSqlReachedTrino("GROUP BY region");
    }

    /** Sort: ORDER BY amount ASC pushed to Trino, order preserved to the sink. */
    @Test
    @Order(7)
    void sort() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");
        List<Record> rows = execute(results -> {
            TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
            SortOperator<Record, Record> sort = new SortOperator<>(
                    new TransformationDescriptor<>(
                            (Record r) -> new Record(r.getField(3)), Record.class, Record.class
                    ).withSqlImplementation("amount", "ASC"),
                    DataSetType.createDefault(Record.class));
            LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
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
        assertSqlReachedTrino("ORDER BY amount ASC");
    }

    /**
     * TableSink: filter + sink composed into a single {@code CREATE TABLE ... AS
     * SELECT} that runs entirely inside Trino — no data leaves the database.
     */
    @Test
    @Order(8)
    void tableSink() throws Exception {
        Assumptions.assumeTrue(trinoAvailable, "Trino not reachable");

        TrinoTableSource src = new TrinoTableSource(ORDERS, "order_id", "customer_id", "region", "amount");
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
        assertSqlReachedTrino("CREATE TABLE " + SINK_TABLE + " AS");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Functional builder: given the result list, wire a plan and return its sink. */
    private interface PlanBuilder {
        LocalCallbackSink<Record> build(List<Record> resultCollector);
    }

    private WayangContext wayangContext() {
        Configuration config = new Configuration();
        config.setProperty("wayang.trino.jdbc.url", JDBC_URL);
        config.setProperty("wayang.trino.jdbc.user", USER);
        config.setProperty("wayang.trino.jdbc.password", "");
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Trino.plugin());
    }

    private List<Record> execute(PlanBuilder builder) {
        List<Record> results = new ArrayList<>();
        LocalCallbackSink<Record> sink = builder.build(results);
        wayangContext().execute(new WayangPlan(sink));
        return results;
    }

    /** Assert that Trino actually ran a query containing the given fragment. */
    private void assertSqlReachedTrino(String fragment) {
        try (Connection c = jdbc()) {
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT count(*) FROM system.runtime.queries "
                  + "WHERE query LIKE '%" + fragment.replace("'", "''") + "%' "
                  + "AND query NOT LIKE '%system.runtime%'");
            rs.next();
            assertTrue(rs.getLong(1) > 0,
                    "Expected a Trino query containing: " + fragment);
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
