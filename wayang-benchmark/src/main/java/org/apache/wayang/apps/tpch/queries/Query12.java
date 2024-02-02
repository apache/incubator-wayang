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

package org.apache.wayang.apps.tpch.queries;

import org.apache.wayang.apps.tpch.data.LineItemTuple;
import org.apache.wayang.apps.tpch.data.OrderTuple;
import org.apache.wayang.apps.tpch.data.q12.QueryResultTuple;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.util.ReflectionUtils;

import java.util.Arrays;

/**
 * Main class for the TPC-H app based on Apache Wayang (incubating).
 */
public class Query12 {

    /**
     * Creates TPC-H Query 12, which is as follows:
     * <pre>
     * SELECT
     *     l_shipmode,
     *     sum(case
     *         when o_orderpriority = '1-URGENT'
     *             OR o_orderpriority = '2-HIGH'
     *             then 1
     *         else 0
     *     end) as high_line_count,
     *     sum(case
     *         when o_orderpriority <> '1-URGENT'
     *             AND o_orderpriority <> '2-HIGH'
     *             then 1
     *         else 0
     *     end) AS low_line_count
     * FROM
     *     orders,
     *     lineitem
     * WHERE
     *     o_orderkey = l_orderkey
     *     AND l_shipmode in ('MAIL', 'SHIP')
     *     AND l_commitdate < l_receiptdate
     *     AND l_shipdate < l_commitdate
     *     AND l_receiptdate >= date '1994-01-01'
     *     AND l_receiptdate < date '1994-01-01' + interval '1' year
     * GROUP BY
     *     l_shipmode
     * ORDER BY
     *     l_shipmode;
     * </pre>
     *
     * @param orderUrl to the order CSV file
     * @param lineItemUrl URL to the lineitem CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link WayangPlan} that implements the query
     */
    public static WayangPlan createPlan(String orderUrl, String lineItemUrl, final int delta) {
        // Read the tables from files.
        TextFileSource orderText = new TextFileSource(orderUrl, "UTF-8");
        TextFileSource lineItemText = new TextFileSource(lineItemUrl, "UTF-8");

        // Parse the rows.
        MapOperator<String, LineItemTuple> lineItemParser = new MapOperator<>(
                (line) -> new LineItemTuple.Parser().parse(line, '|'), String.class, LineItemTuple.class
        );
        lineItemText.connectTo(0, lineItemParser, 0);

        MapOperator<String, OrderTuple> orderParser = new MapOperator<>(
                (line) -> new OrderTuple.Parser().parse(line, '|'), String.class, OrderTuple.class
        );
        orderText.connectTo(0, orderParser, 0);

        // Execute order lineitem Join
        JoinOperator<OrderTuple, LineItemTuple, Integer> olJoin = new JoinOperator<OrderTuple, LineItemTuple, Integer>(
            (order) -> order.L_ORDERKEY,
            (line) -> (int) line.L_ORDERKEY,
            OrderTuple.class,
            LineItemTuple.class,
            Integer.class
        );

        orderParser.connectTo(0, olJoin, 0);
        lineItemParser.connectTo(0, olJoin, 1);

        final int maxDate = OrderTuple.Parser.parseDate("1995-01-01") - delta;
        final int minDate = OrderTuple.Parser.parseDate("1994-01-01") - delta;
        FilterOperator<Tuple2<OrderTuple, LineItemTuple>> filter = new FilterOperator<>(
                ((tuple) -> {
                    String[] shipmodes = {"MAIL", "SHIP"};
                    return (
                        Arrays.asList(shipmodes).contains(tuple.field1.L_SHIPMODE) &&
                        tuple.field1.L_COMMITDATE < tuple.field1.L_RECEIPTDATE &&
                        tuple.field1.L_SHIPDATE < tuple.field1.L_COMMITDATE &&
                        tuple.field1.L_RECEIPTDATE >= minDate &&
                        tuple.field1.L_RECEIPTDATE < maxDate
                    );
                }),
                ReflectionUtils.specify(Tuple2.class)
        );

        olJoin.connectTo(0, filter, 0);

        MapOperator<Tuple2<OrderTuple, LineItemTuple>, QueryResultTuple> resultProjection = new MapOperator<>(
                ((coli) -> {
                    QueryResultTuple result = new QueryResultTuple();
                    result.L_SHIPMODE = coli.field1.L_SHIPMODE;

                    if (coli.field0.L_ORDERPRIORITY.contains("1-URGENT") || coli.field0.L_ORDERPRIORITY.contains("2-HIGH")) {
                        result.HIGH_LINE_COUNT = 1;
                        result.LOW_LINE_COUNT = 0;
                    } else {
                        result.HIGH_LINE_COUNT = 0;
                        result.LOW_LINE_COUNT = 1;
                    }

                    return result;
                }),
                ReflectionUtils.specify(Tuple2.class),
                QueryResultTuple.class
        );

        filter.connectTo(0, resultProjection, 0);

        ReduceByOperator<QueryResultTuple, String> aggregation = new ReduceByOperator<>(
                (returnTuple) -> returnTuple.L_SHIPMODE,
                ((t1, t2) -> {
                    t1.HIGH_LINE_COUNT += t2.HIGH_LINE_COUNT;
                    t1.LOW_LINE_COUNT += t2.LOW_LINE_COUNT;
                    return t1;
                }),
                String.class,
                QueryResultTuple.class
        );

        // Print the results.
        resultProjection.connectTo(0, aggregation, 0);

        SortOperator<QueryResultTuple, String> sort = new SortOperator<>(
                tuple -> tuple.L_SHIPMODE,
                QueryResultTuple.class,
                String.class
        );
        aggregation.connectTo(0, sort, 0);

        LocalCallbackSink<QueryResultTuple> sink = LocalCallbackSink.createStdoutSink(QueryResultTuple.class);
        sort.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}
