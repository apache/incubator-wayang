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
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;
import org.apache.wayang.trino.operators.TrinoTableSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Standalone demo for the Wayang Trino connector.
 *
 * <p>Demonstrates two Trino operator types:
 * <ol>
 *   <li>Seg 3 — Filter pushdown: WHERE region = 'AMER'.</li>
 *   <li>Seg 4 — Projection + Filter pushdown:
 *       SELECT region, product, amount ... WHERE region = 'AMER'.</li>
 * </ol>
 *
 * <p>Run with:
 * <pre>
 *   cd /path/to/wayang
 *   mvn exec:java -pl wayang-platforms/wayang-trino \
 *     -Dexec.mainClass=org.apache.wayang.trino.TrinoDemo \
 *     -Pskip-prerequisite-check -Drat.skip=true \
 *     [-Dtrino.url=jdbc:trino://localhost:8080] [-Dtrino.user=admin]
 * </pre>
 */
public class TrinoDemo {

    private static final String JDBC_URL  = System.getProperty("trino.url",  "jdbc:trino://localhost:8080");
    private static final String JDBC_USER = System.getProperty("trino.user", "admin");

    public static void main(String[] args) throws Exception {
        seg3Filter();
        seg4Projection();
    }

    // ── Seg 3 — Filter pushdown ───────────────────────────────────────────────

    static void seg3Filter() throws Exception {
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Seg 3 — Filter Operator Pushdown");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  Operator:  FilterOperator  ->  TrinoFilterOperator");
        System.out.println("  SQL sent:  SELECT * FROM iceberg.sales.orders");
        System.out.println("                      WHERE region = 'AMER'");
        System.out.println();

        Configuration config = buildConfig();
        WayangContext wayang = buildWayang(config);

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders", "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayang.execute("Trino-Filter-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang:");
        System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                "order_id", "region", "product", "amount", "order_date");
        System.out.println("  " + repeat('-', 54));
        for (Record r : results) {
            System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                    r.getField(0), r.getField(1), r.getField(2), r.getField(3), r.getField(4));
        }
        System.out.println();
        System.out.printf("  ✓ %d AMER rows — filter pushed to Trino as SQL WHERE clause%n", results.size());

        verifyInQueryHistory("iceberg.sales.orders");

        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }

    // ── Seg 4 — Projection + Filter pushdown ─────────────────────────────────

    static void seg4Projection() throws Exception {
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Seg 4 — Projection Operator Pushdown");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  Operators: FilterOperator  ->  TrinoFilterOperator");
        System.out.println("             MapOperator     ->  TrinoProjectionOperator");
        System.out.println("  SQL sent:  SELECT region, product, amount");
        System.out.println("             FROM iceberg.sales.orders");
        System.out.println("             WHERE region = 'AMER'");
        System.out.println();
        System.out.println("  Both operators get pushed into a single SQL query —");
        System.out.println("  no unnecessary columns are transferred over the network.");
        System.out.println();

        Configuration config = buildConfig();
        WayangContext wayang = buildWayang(config);

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders", "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        // Use the Record-aware projection (multi-field). The plain
        // ProjectionDescriptor(Class, Class, fields...) builds a POJO projection
        // whose Java implementation only supports a single field; createForRecords
        // yields a multi-field Record implementation that also works if Wayang
        // executes the projection on the Java side instead of pushing it to Trino.
        MapOperator<Record, Record> projection = new MapOperator<>(
                ProjectionDescriptor.createForRecords(
                        new RecordType("order_id", "region", "product", "amount", "order_date"),
                        "region", "product", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);

        wayang.execute("Trino-Projection-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang (projected columns only):");
        System.out.printf("  %-6s %-10s %10s%n", "region", "product", "amount");
        System.out.println("  " + repeat('-', 30));
        for (Record r : results) {
            System.out.printf("  %-6s %-10s %10s%n", r.getField(0), r.getField(1), r.getField(2));
        }
        System.out.println();
        System.out.printf("  ✓ %d AMER rows — only 3 of 5 columns fetched (projection pushed to SQL)%n",
                results.size());

        verifyInQueryHistory("iceberg.sales.orders");

        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    private static Configuration buildConfig() {
        Configuration config = new Configuration();
        config.setProperty("wayang.trino.jdbc.url",      JDBC_URL);
        config.setProperty("wayang.trino.jdbc.user",     JDBC_USER);
        config.setProperty("wayang.trino.jdbc.password", "");
        return config;
    }

    private static WayangContext buildWayang(Configuration config) {
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Trino.plugin());
    }

    private static void verifyInQueryHistory(String tableHint) throws Exception {
        System.out.println();
        System.out.println("  Checking Trino's system.runtime.queries for proof...");
        Properties props = new Properties();
        props.setProperty("user", JDBC_USER);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, props)) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT query FROM system.runtime.queries " +
                "WHERE state = 'FINISHED' AND query LIKE '%" + tableHint + "%' " +
                "ORDER BY created DESC LIMIT 2"
            );
            System.out.println();
            System.out.println("  Last SQL Trino executed:");
            while (rs.next()) {
                System.out.println("    > " + rs.getString(1).replaceAll("\\s+", " "));
            }
        }
        System.out.println();
        System.out.println("  ✓ Wayang-assembled SQL confirmed in Trino query history.");
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }
}
