package org.apache.wayang.trino;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the local Trino stack.
 *
 * Prerequisites: run `docker-compose up -d` and `./scripts/run-init.sh` first.
 *
 * Run tests:
 *   mvn test -Pintegration
 *
 * Or skip infrastructure setup and run with a custom host:
 *   TRINO_HOST=localhost TRINO_PORT=8080 mvn test -Pintegration
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TrinoIntegrationTest {

    private static final String TRINO_HOST = System.getenv().getOrDefault("TRINO_HOST", "localhost");
    private static final int    TRINO_PORT = Integer.parseInt(System.getenv().getOrDefault("TRINO_PORT", "8080"));
    private static final String JDBC_URL   = String.format("jdbc:trino://%s:%d", TRINO_HOST, TRINO_PORT);

    private static Connection connection;

    // ── Lifecycle ─────────────────────────────────────────────────────────

    @BeforeAll
    static void openConnection() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "admin");   // Trino requires a non-empty user
        connection = DriverManager.getConnection(JDBC_URL, props);
        System.out.printf("Connected to Trino at %s%n", JDBC_URL);
    }

    @AfterAll
    static void closeConnection() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // ── Helper ────────────────────────────────────────────────────────────

    private List<List<Object>> query(String sql) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getObject(i));
                rows.add(row);
            }
        }
        return rows;
    }

    // ── Test 1: Basic connectivity ────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("Trino responds to a simple SELECT 1")
    void testConnectivity() throws SQLException {
        List<List<Object>> rows = query("SELECT 1");
        assertEquals(1, rows.size());
        assertEquals(1L, ((Number) rows.get(0).get(0)).longValue());
        System.out.println("[PASS] Basic connectivity OK");
    }

    // ── Test 2: TPC-H built-in connector ─────────────────────────────────

    @Test
    @Order(2)
    @DisplayName("TPC-H tiny catalog: count orders")
    void testTpchConnector() throws SQLException {
        List<List<Object>> rows = query("SELECT COUNT(*) FROM tpch.tiny.orders");
        long count = ((Number) rows.get(0).get(0)).longValue();
        assertTrue(count > 0, "tpch.tiny.orders should have rows");
        System.out.printf("[PASS] TPC-H tiny.orders has %,d rows%n", count);
    }

    @Test
    @Order(3)
    @DisplayName("TPC-H tiny catalog: top 5 orders by total price")
    void testTpchTopOrders() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT orderkey, totalprice
                FROM tpch.tiny.orders
                ORDER BY totalprice DESC
                LIMIT 5
                """);
        assertEquals(5, rows.size(), "Expected exactly 5 rows");
        System.out.println("[PASS] TPC-H top 5 orders:");
        rows.forEach(r -> System.out.printf("       orderkey=%s  totalprice=%s%n", r.get(0), r.get(1)));
    }

    // ── Test 4: Iceberg — schema exists ──────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("Iceberg catalog: schema 'sales' is visible")
    void testIcebergSchemaVisible() throws SQLException {
        List<List<Object>> rows = query("SHOW SCHEMAS IN iceberg LIKE 'sales'");
        assertFalse(rows.isEmpty(), "Schema 'sales' should exist in iceberg catalog. " +
                "Did you run scripts/run-init.sh?");
        System.out.println("[PASS] Iceberg schema 'sales' is visible");
    }

    // ── Test 5: Iceberg — full table scan ────────────────────────────────

    @Test
    @Order(5)
    @DisplayName("Iceberg table: select all orders")
    void testIcebergSelectAll() throws SQLException {
        List<List<Object>> rows = query("SELECT * FROM iceberg.sales.orders ORDER BY order_id");
        assertEquals(20, rows.size(), "Expected 20 rows inserted by init.sql");
        System.out.println("[PASS] Iceberg full scan: 20 rows");
        rows.forEach(r -> System.out.printf("       %s%n", r));
    }

    // ── Test 6: Iceberg — pushdown filter ────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("Iceberg table: filter by region = APAC")
    void testIcebergFilterByRegion() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT order_id, region, amount
                FROM iceberg.sales.orders
                WHERE region = 'APAC'
                ORDER BY order_id
                """);
        assertFalse(rows.isEmpty(), "Should have APAC orders");
        rows.forEach(r -> assertEquals("APAC", r.get(1), "All rows must be APAC"));
        System.out.printf("[PASS] Filter pushdown: %d APAC rows%n", rows.size());
    }

    // ── Test 7: Iceberg — aggregation ────────────────────────────────────

    @Test
    @Order(7)
    @DisplayName("Iceberg table: aggregate total_amount by region")
    void testIcebergAggregate() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT region, COUNT(*) AS order_count, SUM(amount) AS total_amount
                FROM iceberg.sales.orders
                GROUP BY region
                ORDER BY total_amount DESC
                """);
        assertFalse(rows.isEmpty(), "Aggregation should return rows");
        System.out.println("[PASS] Aggregation by region:");
        rows.forEach(r -> System.out.printf("       region=%-5s  count=%s  total=%.2f%n",
                r.get(0), r.get(1), ((Number) r.get(2)).doubleValue()));
    }

    // ── Test 8: Iceberg — amount threshold filter ─────────────────────────

    @Test
    @Order(8)
    @DisplayName("Iceberg table: filter orders with amount > 1000")
    void testIcebergFilterByAmount() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT order_id, amount
                FROM iceberg.sales.orders
                WHERE amount > 1000.0
                ORDER BY amount DESC
                """);
        rows.forEach(r -> assertTrue(
                ((Number) r.get(1)).doubleValue() > 1000.0,
                "All rows must have amount > 1000"
        ));
        System.out.printf("[PASS] Amount filter: %d rows with amount > 1000%n", rows.size());
    }

    // ── Test 9: Iceberg — projection (select subset of columns) ───────────

    @Test
    @Order(9)
    @DisplayName("Iceberg table: project only region and product columns")
    void testIcebergProjection() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT region, product
                FROM iceberg.sales.orders
                LIMIT 5
                """);
        assertEquals(5, rows.size());
        rows.forEach(r -> {
            assertNotNull(r.get(0), "region should not be null");
            assertNotNull(r.get(1), "product should not be null");
        });
        System.out.println("[PASS] Projection (region, product): 5 rows returned");
    }

    // ── Test 10: Iceberg metadata — table files ──────────────────────────

    @Test
    @Order(10)
    @DisplayName("Iceberg metadata: $files table lists at least one Parquet file")
    void testIcebergFilesMetadata() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT file_path, record_count
                FROM iceberg.sales."orders$files"
                """);
        assertFalse(rows.isEmpty(), "There should be at least one data file after inserts");
        System.out.println("[PASS] Iceberg $files metadata:");
        rows.forEach(r -> System.out.printf("       %s  (records=%s)%n", r.get(0), r.get(1)));
    }
}
