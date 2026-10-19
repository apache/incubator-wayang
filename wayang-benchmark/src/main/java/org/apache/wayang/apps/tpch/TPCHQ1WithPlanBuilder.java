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

package org.apache.wayang.apps.tpch;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.apps.tpch.data.LineItemTuple;
import org.apache.wayang.apps.tpch.data.q1.GroupKey;
import org.apache.wayang.apps.tpch.data.q1.ReturnTuple;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

import java.util.Collection;

/**
 * TPC-H Query 1 implementation using JavaPlanBuilder API.
 * This is the modern, fluent API version. Compare with {@link TPCHQ1WithJavaNative}
 * to see the differences between the native operator API and the JavaPlanBuilder API.
 */
public class TPCHQ1WithPlanBuilder {

    /**
     * Executes TPC-H Query 1, which is as follows:
     * <pre>
     * select
     *  l_returnflag,
     *  l_linestatus,
     *  sum(l_quantity) as sum_qty,
     *  sum(l_extendedprice) as sum_base_price,
     *  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
     *  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
     *  avg(l_quantity) as avg_qty,
     *  avg(l_extendedprice) as avg_price,
     *  avg(l_discount) as avg_disc,
     *  count(*) as count_order
     * from
     *  lineitem
     * where
     *  l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
     * group by
     *  l_returnflag,
     *  l_linestatus
     * order by
     *  l_returnflag,
     *  l_linestatus;
     * </pre>
     *
     * @param wayangContext the Wayang context
     * @param lineItemUrl   URL to the lineitem CSV file
     * @param delta         the {@code [DELTA]} parameter
     * @return Collection of query results
     */
    private static Collection<ReturnTuple> executeQ1(WayangContext wayangContext, String lineItemUrl, final int delta) {
        final int maxShipdate = LineItemTuple.Parser.parseDate("1998-12-01") - delta;

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("TPC-H Q1")
                .withUdfJarOf(TPCHQ1WithPlanBuilder.class);

        return planBuilder
                // Read the lineitem table
                .readTextFile(lineItemUrl).withName("Load lineitem")

                // Parse the rows
                .map(line -> new LineItemTuple.Parser().parse(line))
                .withName("Parse lineitem")

                // Filter by shipdate
                .filter(tuple -> tuple.L_SHIPDATE <= maxShipdate)
                .withName("Filter by shipdate")

                // Project the queried attributes
                .map(lineItemTuple -> new ReturnTuple(
                        lineItemTuple.L_RETURNFLAG,
                        lineItemTuple.L_LINESTATUS,
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT),
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT) * (1 + lineItemTuple.L_TAX),
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_DISCOUNT,
                        1))
                .withName("Project attributes")

                // Aggregation: group by returnflag and linestatus
                .reduceByKey(
                        returnTuple -> new GroupKey(returnTuple.L_RETURNFLAG, returnTuple.L_LINESTATUS),
                        (t1, t2) -> {
                            t1.SUM_QTY += t2.SUM_QTY;
                            t1.SUM_BASE_PRICE += t2.SUM_BASE_PRICE;
                            t1.SUM_DISC_PRICE += t2.SUM_DISC_PRICE;
                            t1.SUM_CHARGE += t2.SUM_CHARGE;
                            t1.AVG_QTY += t2.AVG_QTY;
                            t1.AVG_PRICE += t2.AVG_PRICE;
                            t1.AVG_DISC += t2.AVG_DISC;
                            t1.COUNT_ORDER += t2.COUNT_ORDER;
                            return t1;
                        })
                .withName("Aggregate")

                // Finalize AVG operations
                .map(t -> {
                    t.AVG_QTY /= t.COUNT_ORDER;
                    t.AVG_PRICE /= t.COUNT_ORDER;
                    t.AVG_DISC /= t.COUNT_ORDER;
                    return t;
                })
                .withName("Finalize aggregation")

                // Execute and collect results
                .collect();
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <query number> <query parameters>*");
            System.exit(1);
        }

        WayangContext wayangContext = new WayangContext(new Configuration());
        for (String platform : args[0].split(",")) {
            switch (platform) {
                case "java":
                    wayangContext.register(Java.basicPlugin());
                    break;
                case "spark":
                    wayangContext.register(Spark.basicPlugin());
                    break;
                default:
                    System.err.format("Unknown platform: \"%s\"\n", platform);
                    System.exit(3);
                    return;
            }
        }

        Collection<ReturnTuple> results;
        switch (Integer.parseInt(args[1])) {
            case 1:
                results = executeQ1(wayangContext, args[2], Integer.parseInt(args[3]));
                break;
            default:
                System.err.println("Unsupported query number.");
                System.exit(2);
                return;
        }

        // Print results
        results.forEach(System.out::println);
    }
}
