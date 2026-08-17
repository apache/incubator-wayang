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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.types.RecordType;
import org.apache.wayang.bigquery.operators.BigQueryTableSource;
import org.apache.wayang.bigquery.platform.BigQueryPlatform;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.Java;

import java.util.ArrayList;
import java.util.List;

/**
 * Standalone demo for the Wayang BigQuery connector.
 *
 * <p>Controlled by {@code -Dbigquery.mode}:
 * <ul>
 *   <li>{@code cost}       — three-layer cost model (no credentials needed)</li>
 *   <li>{@code filter}     — filter operator pushdown demo</li>
 *   <li>{@code projection} — projection + filter operator pushdown demo</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   mvn exec:java -pl wayang-platforms/wayang-bigquery \
 *     -Dexec.mainClass=org.apache.wayang.bigquery.BigQueryDemo \
 *     -Dbigquery.mode=cost \
 *     -Pskip-prerequisite-check -Drat.skip=true
 * </pre>
 */
public class BigQueryDemo {

    private static final String MODE     = System.getProperty("bigquery.mode", "cost");
    private static final String JDBC_URL = System.getProperty("bigquery.url",  "");
    private static final String PROJECT  = System.getProperty("bigquery.project", "my-project");

    // 20-row dataset: 4 regions (AMER/APAC/EMEA/LATAM), 5 products (Widget A-E)
    // AMER rows: 3, 6, 9, 12, 16  →  5 rows for filter demo
    private static final String[][] SAMPLE_DATA = {
        {"1",  "APAC",  "Widget A", "1500.00", "2024-01-15"},
        {"2",  "EMEA",  "Widget B",  "800.50", "2024-01-16"},
        {"3",  "AMER",  "Widget A", "2200.00", "2024-01-17"},
        {"4",  "APAC",  "Widget C",  "350.75", "2024-01-18"},
        {"5",  "EMEA",  "Widget A", "1100.00", "2024-01-19"},
        {"6",  "AMER",  "Widget B",  "950.25", "2024-01-20"},
        {"7",  "APAC",  "Widget B", "1750.00", "2024-01-21"},
        {"8",  "EMEA",  "Widget C",  "420.00", "2024-01-22"},
        {"9",  "AMER",  "Widget C",  "680.50", "2024-01-23"},
        {"10", "APAC",  "Widget A", "3000.00", "2024-01-24"},
        {"11", "LATAM", "Widget D",  "560.00", "2024-01-25"},
        {"12", "AMER",  "Widget D", "1320.75", "2024-01-26"},
        {"13", "EMEA",  "Widget D",  "990.00", "2024-01-27"},
        {"14", "LATAM", "Widget E", "2100.50", "2024-01-28"},
        {"15", "APAC",  "Widget E", "4500.00", "2024-01-29"},
        {"16", "AMER",  "Widget E", "3750.00", "2024-01-30"},
        {"17", "EMEA",  "Widget E", "1250.00", "2024-01-31"},
        {"18", "LATAM", "Widget A",  "870.25", "2024-02-01"},
        {"19", "APAC",  "Widget D", "1680.00", "2024-02-02"},
        {"20", "LATAM", "Widget B",  "440.50", "2024-02-03"},
    };

    public static void main(String[] args) {
        switch (MODE) {
            case "cost":       costModel();    break;
            case "filter":     filterDemo();   break;
            case "projection": projectionDemo(); break;
            default:
                costModel();
                filterDemo();
                projectionDemo();
        }
    }

    // ── Cost model ────────────────────────────────────────────────────────────

    static void costModel() {
        Configuration config = new Configuration();
        BigQueryPlatform.getInstance().configureDefaults(config);

        long   mhz   = config.getLongProperty("wayang.bigquery.cpu.mhz",        0);
        long   cores = config.getLongProperty("wayang.bigquery.cores",           0);
        double fix   = config.getDoubleProperty("wayang.bigquery.costs.fix",     0);
        double perMs = config.getDoubleProperty("wayang.bigquery.costs.per-ms",  1);

        long   rows      = 10;
        long   alpha     = 5;
        long   beta      = 2_000_000;
        long   cpuCycles = alpha * rows + beta;
        double timeMs    = cpuCycles / (cores * mhz * 1000.0);
        double cost      = fix + perMs * timeMs;

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  BigQuery — Cost Model Integration");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  LAYER 1 — Cost formula  (wayang-bigquery-defaults.properties)");
        System.out.printf("    tablesource : %s%n", config.getStringProperty("wayang.bigquery.tablesource.load", null));
        System.out.printf("    filter      : %s%n", config.getStringProperty("wayang.bigquery.filter.load",      null));
        System.out.println();
        System.out.println("  LAYER 2 — Hardware profile  (cpu cycles -> wall-clock ms)");
        System.out.printf("    cpu.mhz = %d   cores = %d%n", mhz, cores);
        System.out.println();
        System.out.println("  LAYER 3 — Time -> abstract cost");
        System.out.printf("    costs.fix = %.1f   costs.per-ms = %.1f%n", fix, perMs);
        System.out.println();
        System.out.println("  -- Worked example: 10-row table scan --");
        System.out.printf("    alpha = %d  (per-row, serverless columnar)%n", alpha);
        System.out.printf("    beta  = %,d  (cold-start / slot reservation)%n", beta);
        System.out.printf("    cpu cycles = %d * %d + %,d = %,d%n", alpha, rows, beta, cpuCycles);
        System.out.printf("    time       = %,d / (%d * %d * 1000) = %.4f ms%n", cpuCycles, cores, mhz, timeMs);
        System.out.printf("    cost       = %.1f + %.1f * %.4f = %.4f%n", fix, perMs, timeMs, cost);
        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }

    // ── Filter pushdown ───────────────────────────────────────────────────────

    static void filterDemo() {
        String table = String.format("`%s.sales.orders`", PROJECT);

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  BigQuery — Filter Operator Pushdown");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  Operator:  FilterOperator  ->  BigQueryFilterOperator");
        System.out.printf("  SQL sent:  SELECT * FROM %s%n", table);
        System.out.println("                      WHERE region = 'AMER'");
        System.out.println();

        if (!JDBC_URL.isEmpty()) {
            runLiveFilter(table);
        } else {
            System.out.println("  Results (20-row dataset, AMER rows only):");
            System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                    "order_id", "region", "product", "amount", "order_date");
            System.out.println("  " + repeat('-', 54));
            int count = 0;
            for (String[] row : SAMPLE_DATA) {
                if ("AMER".equals(row[1])) {
                    System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                            row[0], row[1], row[2], row[3], row[4]);
                    count++;
                }
            }
            System.out.println();
            System.out.printf("  ✓ %d AMER rows — filter pushed to BigQuery as SQL WHERE%n", count);
            System.out.println("    (pass -Dbigquery.url=... for live execution)");
        }

        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }

    private static void runLiveFilter(String table) {
        WayangContext wayang = buildWayang();
        List<Record> results = new ArrayList<>();

        BigQueryTableSource source = new BigQueryTableSource(
                table, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);
        wayang.execute("BigQuery-Filter-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang:");
        System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                "order_id", "region", "product", "amount", "order_date");
        System.out.println("  " + repeat('-', 54));
        for (Record r : results) {
            System.out.printf("  %-10s %-6s %-10s %10s %-12s%n",
                    r.getField(0), r.getField(1), r.getField(2), r.getField(3), r.getField(4));
        }
        System.out.println();
        System.out.printf("  ✓ %d AMER rows via Wayang -> BigQuery SQL pushdown%n%n", results.size());
    }

    // ── Projection + Filter pushdown ──────────────────────────────────────────

    static void projectionDemo() {
        String table = String.format("`%s.sales.orders`", PROJECT);

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  BigQuery — Projection Operator Pushdown");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  Operators: FilterOperator  ->  BigQueryFilterOperator");
        System.out.println("             MapOperator     ->  BigQueryProjectionOperator");
        System.out.printf("  SQL sent:  SELECT region, product, amount%n");
        System.out.printf("             FROM %s%n", table);
        System.out.println("             WHERE region = 'AMER'");
        System.out.println();
        System.out.println("  Both operators collapsed into one SQL — only 3 of 5");
        System.out.println("  columns transferred; order_id + order_date never leave BQ.");
        System.out.println();

        if (!JDBC_URL.isEmpty()) {
            runLiveProjection(table);
        } else {
            System.out.println("  Results (projected: region, product, amount — AMER only):");
            System.out.printf("  %-6s %-10s %10s%n", "region", "product", "amount");
            System.out.println("  " + repeat('-', 30));
            int count = 0;
            for (String[] row : SAMPLE_DATA) {
                if ("AMER".equals(row[1])) {
                    System.out.printf("  %-6s %-10s %10s%n", row[1], row[2], row[3]);
                    count++;
                }
            }
            System.out.println();
            System.out.printf("  ✓ %d AMER rows, 3 columns — projection + filter pushed to BigQuery SQL%n",
                    count);
            System.out.println("    (pass -Dbigquery.url=... for live execution)");
        }

        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }

    private static void runLiveProjection(String table) {
        WayangContext wayang = buildWayang();
        List<Record> results = new ArrayList<>();

        BigQueryTableSource source = new BigQueryTableSource(
                table, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        // Record-aware multi-field projection (see TrinoDemo for rationale).
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
        wayang.execute("BigQuery-Projection-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang (projected columns only):");
        System.out.printf("  %-6s %-10s %10s%n", "region", "product", "amount");
        System.out.println("  " + repeat('-', 30));
        for (Record r : results) {
            System.out.printf("  %-6s %-10s %10s%n", r.getField(0), r.getField(1), r.getField(2));
        }
        System.out.println();
        System.out.printf("  ✓ %d AMER rows, 3 columns — projection + filter pushed to BigQuery SQL%n%n",
                results.size());
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    private static WayangContext buildWayang() {
        Configuration config = new Configuration();
        config.setProperty("wayang.bigquery.jdbc.url", JDBC_URL);
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(BigQuery.plugin());
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }
}
