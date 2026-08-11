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

package org.apache.wayang.duckdb;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Standalone JDBC integration tests for the local DuckDB setup.
 *
 * <p>Run from the repository root with:
 * <pre>
 *   ./mvnw -f platforms-setup-guides/duckdb-setup/pom.xml \
 *     -Pintegration -Dtest=DuckDBIntegrationTest test
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DuckDBIntegrationTest {

    private static final String JDBC_URL = System.getenv().getOrDefault(
            "DUCKDB_JDBC_URL",
            System.getProperty("duckdb.url", "jdbc:duckdb:data/wayang.duckdb"));

    private static Connection connection;

    @BeforeAll
    static void openConnectionAndLoadFixture() throws Exception {
        connection = DriverManager.getConnection(JDBC_URL);
        executeSqlScript(Path.of("scripts", "init.sql"));
    }

    @AfterAll
    static void closeConnection() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("DuckDB responds to SELECT 1")
    void connectivity() throws SQLException {
        List<List<Object>> rows = query("SELECT 1");
        assertEquals(1, rows.size());
        assertEquals(1, ((Number) rows.get(0).get(0)).intValue());
    }

    @Test
    @Order(2)
    @DisplayName("Fixture tables are visible")
    void fixtureTablesVisible() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'wayang_it'
                ORDER BY table_name
                """);
        assertEquals(2, rows.size());
        assertEquals("customers", rows.get(0).get(0));
        assertEquals("orders", rows.get(1).get(0));
    }

    @Test
    @Order(3)
    @DisplayName("Orders table full scan")
    void ordersFullScan() throws SQLException {
        assertEquals(6L, scalarLong("SELECT COUNT(*) FROM wayang_it.orders"));
    }

    @Test
    @Order(4)
    @DisplayName("Filter by region")
    void filterByRegion() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT order_id, region
                FROM wayang_it.orders
                WHERE region = 'AMER'
                ORDER BY order_id
                """);
        assertEquals(3, rows.size());
        rows.forEach(row -> assertEquals("AMER", row.get(1)));
    }

    @Test
    @Order(5)
    @DisplayName("Project subset of columns")
    void projection() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT region, amount
                FROM wayang_it.orders
                ORDER BY order_id
                LIMIT 3
                """);
        assertEquals(3, rows.size());
        assertEquals(2, rows.get(0).size());
    }

    @Test
    @Order(6)
    @DisplayName("Join orders and customers")
    void join() throws SQLException {
        assertEquals(6L, scalarLong("""
                SELECT COUNT(*)
                FROM wayang_it.orders o
                JOIN wayang_it.customers c ON o.customer_id = c.cust_id
                """));
    }

    @Test
    @Order(7)
    @DisplayName("Aggregate total amount by region")
    void aggregateByRegion() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT region, SUM(amount) AS total_amount
                FROM wayang_it.orders
                GROUP BY region
                ORDER BY region
                """);
        assertEquals(3, rows.size());
        assertEquals("AMER", rows.get(0).get(0));
        assertEquals(3830.75, ((Number) rows.get(0).get(1)).doubleValue(), 0.01);
    }

    @Test
    @Order(8)
    @DisplayName("Filter by amount threshold")
    void filterByAmount() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT amount
                FROM wayang_it.orders
                WHERE amount > 1000.0
                """);
        assertFalse(rows.isEmpty());
        rows.forEach(row -> assertTrue(((Number) row.get(0)).doubleValue() > 1000.0));
    }

    @Test
    @Order(9)
    @DisplayName("Sort by amount")
    void sortByAmount() throws SQLException {
        List<List<Object>> rows = query("""
                SELECT order_id, amount
                FROM wayang_it.orders
                ORDER BY amount DESC
                LIMIT 1
                """);
        assertEquals(1, rows.size());
        assertEquals(1L, ((Number) rows.get(0).get(0)).longValue());
    }

    @Test
    @Order(10)
    @DisplayName("Create table as select")
    void createTableAsSelect() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS wayang_it.operator_result");
            statement.execute("""
                    CREATE TABLE wayang_it.operator_result AS
                    SELECT * FROM wayang_it.orders WHERE region = 'AMER'
                    """);
        }
        assertEquals(3L, scalarLong("SELECT COUNT(*) FROM wayang_it.operator_result"));
    }

    private static void executeSqlScript(Path script) throws Exception {
        String sql = Files.readString(script);
        StringBuilder statement = new StringBuilder();
        try (Statement jdbcStatement = connection.createStatement()) {
            for (String line : sql.split("\\R")) {
                String trimmed = line.trim();
                if (trimmed.startsWith("--") || trimmed.isEmpty()) {
                    continue;
                }
                statement.append(line).append('\n');
                if (trimmed.endsWith(";")) {
                    jdbcStatement.execute(statement.toString());
                    statement.setLength(0);
                }
            }
            if (statement.length() > 0) {
                jdbcStatement.execute(statement.toString());
            }
        }
    }

    private static long scalarLong(String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static List<List<Object>> query(String sql) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            int columns = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    row.add(resultSet.getObject(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }
}
