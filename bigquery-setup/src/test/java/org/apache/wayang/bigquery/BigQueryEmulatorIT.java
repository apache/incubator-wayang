package org.apache.wayang.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the local BigQuery emulator.
 *
 * Prerequisites: run `docker-compose up -d` first.
 *
 * Run tests:
 *   mvn test -Pintegration
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryEmulatorIT {

    private static final String EMULATOR_HOST = System.getenv().getOrDefault("BIGQUERY_HOST", "http://localhost:9050");
    private static final String PROJECT_ID = "test-project";
    private static final String DATASET = "sales";

    private static BigQuery bigquery;
    private static boolean emulatorAvailable = false;

    @BeforeAll
    static void setupClient() {
        try {
            bigquery = BigQueryOptions.newBuilder()
                    .setHost(EMULATOR_HOST)
                    .setLocation("US")
                    .setProjectId(PROJECT_ID)
                    .setCredentials(NoCredentials.getInstance())
                    .build()
                    .getService();

            // Quick connectivity check
            bigquery.getDataset(DatasetId.of(PROJECT_ID, DATASET));
            emulatorAvailable = true;
            System.out.printf("Connected to BigQuery emulator at %s%n", EMULATOR_HOST);
        } catch (Exception e) {
            System.err.println("BigQuery emulator not available: " + e.getMessage());
        }
    }

    private List<List<Object>> runQuery(String sql) throws InterruptedException {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
                .setUseLegacySql(false)
                .build();
        TableResult result = bigquery.query(config);
        List<List<Object>> rows = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            List<Object> r = new ArrayList<>();
            for (FieldValue val : row) {
                r.add(val.isNull() ? null : val.getValue());
            }
            rows.add(r);
        }
        return rows;
    }

    // ── Test 1: Dataset visible ──────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("BigQuery emulator: dataset 'sales' is visible")
    void testDatasetVisible() {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        Dataset ds = bigquery.getDataset(DatasetId.of(PROJECT_ID, DATASET));
        assertNotNull(ds, "Dataset 'sales' should exist");
        System.out.println("[PASS] Dataset 'sales' is visible");
    }

    // ── Test 2: Full table scan ──────────────────────────────────────────

    @Test
    @Order(2)
    @DisplayName("BigQuery emulator: full table scan on orders")
    void testFullScan() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        List<List<Object>> rows = runQuery(
                "SELECT * FROM `test-project.sales.orders` ORDER BY order_id"
        );
        assertEquals(10, rows.size(), "Expected 10 rows");
        System.out.println("[PASS] Full scan: " + rows.size() + " rows");
        rows.forEach(r -> System.out.println("       " + r));
    }

    // ── Test 3: Filter by region ─────────────────────────────────────────

    @Test
    @Order(3)
    @DisplayName("BigQuery emulator: filter by region = APAC")
    void testFilterByRegion() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        List<List<Object>> rows = runQuery(
                "SELECT order_id, region, amount FROM `test-project.sales.orders` WHERE region = 'APAC' ORDER BY order_id"
        );
        assertFalse(rows.isEmpty(), "Should have APAC rows");
        rows.forEach(r -> assertEquals("APAC", r.get(1), "All rows should be APAC"));
        System.out.printf("[PASS] Filter: %d APAC rows%n", rows.size());
    }

    // ── Test 4: Filter by amount ─────────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("BigQuery emulator: filter by amount > 1000")
    void testFilterByAmount() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        List<List<Object>> rows = runQuery(
                "SELECT order_id, amount FROM `test-project.sales.orders` WHERE amount > 1000 ORDER BY amount DESC"
        );
        assertFalse(rows.isEmpty());
        rows.forEach(r -> assertTrue(
                Double.parseDouble(r.get(1).toString()) > 1000.0,
                "All amounts should be > 1000"
        ));
        System.out.printf("[PASS] Amount filter: %d rows with amount > 1000%n", rows.size());
    }

    // ── Test 5: Aggregation ──────────────────────────────────────────────

    @Test
    @Order(5)
    @DisplayName("BigQuery emulator: aggregate by region")
    void testAggregation() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        List<List<Object>> rows = runQuery(
                "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total " +
                "FROM `test-project.sales.orders` GROUP BY region ORDER BY total DESC"
        );
        assertFalse(rows.isEmpty());
        System.out.println("[PASS] Aggregation by region:");
        rows.forEach(r -> System.out.printf("       region=%-5s  count=%s  total=%s%n",
                r.get(0), r.get(1), r.get(2)));
    }

    // ── Test 6: Projection ───────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("BigQuery emulator: projection (region, product)")
    void testProjection() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        List<List<Object>> rows = runQuery(
                "SELECT region, product FROM `test-project.sales.orders` LIMIT 5"
        );
        assertEquals(5, rows.size());
        rows.forEach(r -> {
            assertNotNull(r.get(0), "region should not be null");
            assertNotNull(r.get(1), "product should not be null");
        });
        System.out.println("[PASS] Projection (region, product): 5 rows");
    }

    // ── Test 7: COUNT(*) ─────────────────────────────────────────────────

    @Test
    @Order(7)
    @DisplayName("BigQuery emulator: SELECT count(*)")
    void testCount() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        List<List<Object>> rows = runQuery(
                "SELECT count(*) FROM `test-project.sales.orders`"
        );
        assertEquals(1, rows.size());
        long count = Long.parseLong(rows.get(0).get(0).toString());
        assertEquals(10, count, "Should have 10 rows");
        System.out.println("[PASS] COUNT(*) = " + count);
    }
}
