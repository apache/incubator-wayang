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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.duckdb.operators.DuckDBTableSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Standalone demo for the Wayang DuckDB platform.
 *
 * <p>Run from the repository root with:
 * <pre>
 *   ./mvnw -Pskip-prerequisite-check -pl wayang-platforms/wayang-duckdb \
 *     -DskipTests -Drat.skip=true -Dlicense.skip=true exec:java \
 *     -Dexec.mainClass=org.apache.wayang.duckdb.DuckDBDemo
 * </pre>
 */
public class DuckDBDemo {

    private static final String JDBC_URL = System.getProperty("duckdb.url", "jdbc:duckdb:target/duckdb-demo.duckdb");
    private static final String SCHEMA = "wayang_demo";
    private static final String ORDERS = SCHEMA + ".orders";
    private static final String FILTER_RESULT = SCHEMA + ".filter_result";
    private static final String PROJECTION_RESULT = SCHEMA + ".projection_result";

    public static void main(String[] args) throws Exception {
        createFixture();
        runFilterPushdown();
        runProjectionPushdown();
    }

    private static void runFilterPushdown() throws Exception {
        System.out.println();
        System.out.println("DuckDB demo: Filter pushdown");
        System.out.println("SQL shape: SELECT * FROM " + ORDERS + " WHERE region = 'AMER'");

        DuckDBTableSource source = new DuckDBTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record record) -> "AMER".equals(record.getField(2)), Record.class)
                        .withSqlImplementation("region = 'AMER'"));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", FILTER_RESULT,
                "order_id", "customer_id", "region", "amount");

        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);
        wayangContext().execute("DuckDB filter demo", new WayangPlan(sink));

        printQuery("SELECT order_id, region, amount FROM " + FILTER_RESULT + " ORDER BY order_id");
    }

    private static void runProjectionPushdown() throws Exception {
        System.out.println();
        System.out.println("DuckDB demo: Projection + filter pushdown");
        System.out.println("SQL shape: SELECT region, amount FROM " + ORDERS + " WHERE region = 'AMER'");

        DuckDBTableSource source = new DuckDBTableSource(
                ORDERS, "order_id", "customer_id", "region", "amount");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (Record record) -> "AMER".equals(record.getField(2)), Record.class)
                        .withSqlImplementation("region = 'AMER'"));
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "customer_id", "region", "amount"),
                        "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class));
        TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", PROJECTION_RESULT,
                "region", "amount");

        source.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);
        wayangContext().execute("DuckDB projection demo", new WayangPlan(sink));

        printQuery("SELECT region, amount FROM " + PROJECTION_RESULT + " ORDER BY amount DESC");
    }

    private static WayangContext wayangContext() {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.duckdb.jdbc.url", JDBC_URL);
        configuration.setProperty("wayang.duckdb.jdbc.user", "");
        configuration.setProperty("wayang.duckdb.jdbc.password", "");
        return new WayangContext(configuration)
                .withPlugin(DuckDB.plugin());
    }

    private static void createFixture() throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            statement.execute("DROP TABLE IF EXISTS " + FILTER_RESULT);
            statement.execute("DROP TABLE IF EXISTS " + PROJECTION_RESULT);
            statement.execute("DROP TABLE IF EXISTS " + ORDERS);
            statement.execute("CREATE TABLE " + ORDERS + " ("
                    + "order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE)");
            statement.execute("INSERT INTO " + ORDERS + " VALUES "
                    + "(1, 100, 'AMER', 2200.0),"
                    + "(2, 101, 'EMEA',  800.5),"
                    + "(3, 100, 'AMER',  680.5),"
                    + "(4, 102, 'APAC', 1500.0),"
                    + "(5, 101, 'EMEA', 1100.0),"
                    + "(6, 100, 'AMER',  950.25)");
        }
    }

    private static void printQuery(String sql) throws Exception {
        try (Connection connection = DriverManager.getConnection(JDBC_URL);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            int columns = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                StringBuilder row = new StringBuilder("  ");
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) {
                        row.append(" | ");
                    }
                    row.append(resultSet.getObject(i));
                }
                System.out.println(row);
            }
        }
    }
}
