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
import org.apache.wayang.apps.tpch.data.CustomerTuple;
import org.apache.wayang.apps.tpch.data.q3.LineItemReturnTuple;
import org.apache.wayang.apps.tpch.data.q3.OrderReturnTuple;
import org.apache.wayang.apps.tpch.data.q3.QueryResultTuple;
import org.apache.wayang.apps.tpch.data.q3.GroupKey;
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
/**
 * Main class for the TPC-H app based on Apache Wayang (incubating).
 */
public class Query3 {

    /**
     * Creates TPC-H Query 3, which is as follows:
     * <pre>
     * SELECT
     *   l_orderkey,
     *   sum(l_extendedprice * (1 - l_discount)) as revenue,
     *   o_orderdate,
     *   o_shippriority
     * FROM
     *   customer,
     *   orders,
     *   lineitem
     * WHERE
     *   c_mktsegment = 'BUILDING'
     *   AND c_custkey = o_custkey
     *   AND l_orderkey = o_orderkey
     *   AND o_orderdate < date '1995-03-15'
     *   AND l_shipdate > date '1995-03-15'
     * GROUP BY
     *   l_orderkey,
     *   o_orderdate,
     *   o_shippriority
     * ORDER BY
     *   revenue desc,
     *   o_orderdate
     * LIMIT 20;
     * </pre>
     *
     * @param customer URL to the customer CSV file
     * @param order URL to the order CSV file
     * @param lineItemUrl URL to the lineitem CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link WayangPlan} that implements the query
     */
    public static WayangPlan createPlan(String customerUrl, String orderUrl, String lineItemUrl, final int delta) {
        // Read the tables from files.
        TextFileSource customerText = new TextFileSource(customerUrl, "UTF-8");
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

        MapOperator<String, CustomerTuple> customerParser = new MapOperator<>(
                (line) -> new CustomerTuple.Parser().parse(line, '|'), String.class, CustomerTuple.class
        );
        customerText.connectTo(0, customerParser, 0);

        // Filter customers.
        FilterOperator<CustomerTuple> customerFilter = new FilterOperator<>(
                (tuple) -> tuple.L_MKTSEGMENT.contains("BUILDING"), CustomerTuple.class
        );
        customerParser.connectTo(0, customerFilter, 0);

        MapOperator<CustomerTuple, Integer> customerProjection = new MapOperator<>(
                (lineItemTuple) -> Integer.valueOf(lineItemTuple.L_CUSTKEY),
                CustomerTuple.class,
                Integer.class
        );
        customerFilter.connectTo(0, customerProjection, 0);

        // Filter by shipdate.
        final int minShipdate = LineItemTuple.Parser.parseDate("1995-03-15") - delta;
        FilterOperator<LineItemTuple> lineFilter = new FilterOperator<>(
                (tuple) -> tuple.L_SHIPDATE > minShipdate, LineItemTuple.class
        );
        lineItemParser.connectTo(0, lineFilter, 0);

        // Project the queried attributes.
        MapOperator<LineItemTuple, LineItemReturnTuple> lineProjection = new MapOperator<>(
                (lineItemTuple) -> new LineItemReturnTuple(
                        lineItemTuple.L_ORDERKEY,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT)
                ),
                LineItemTuple.class,
                LineItemReturnTuple.class
        );
        lineFilter.connectTo(0, lineProjection, 0);

        // Filter by orderdate.
        final int maxOrderdate = OrderTuple.Parser.parseDate("1995-03-15") - delta;
        FilterOperator<OrderTuple> orderFilter = new FilterOperator<>(
                (tuple) -> tuple.L_ORDERDATE < maxOrderdate, OrderTuple.class
        );
        orderParser.connectTo(0, orderFilter, 0);

        // Project the queried attributes.
        MapOperator<OrderTuple, OrderReturnTuple> orderProjection = new MapOperator<>(
                (orderTuple) -> new OrderReturnTuple(
                        orderTuple.L_ORDERKEY,
                        orderTuple.L_CUSTKEY,
                        orderTuple.L_ORDERDATE,
                        orderTuple.L_SHIPPRIORITY
                ),
                OrderTuple.class,
                OrderReturnTuple.class
        );
        orderFilter.connectTo(0, orderProjection, 0);

        // Execute customer-order Join
        JoinOperator<Integer, OrderReturnTuple, Integer> coJoin = new JoinOperator<Integer, OrderReturnTuple, Integer>(
            (customer) -> customer,
            (order) -> order.O_CUSTKEY,
            Integer.class,
            OrderReturnTuple.class,
            Integer.class
        );

        customerProjection.connectTo(0, coJoin, 0);
        orderProjection.connectTo(0, coJoin, 1);

        // Execute customer-order with line-items Join
        JoinOperator<LineItemReturnTuple, Tuple2<Integer, OrderReturnTuple>, Integer> coLJoin = new JoinOperator<LineItemReturnTuple, Tuple2<Integer, OrderReturnTuple>, Integer>(
            (lineItemReturnTuple) -> (int) lineItemReturnTuple.L_ORDERKEY,
            (tuple) -> tuple.field1.O_ORDERKEY,
            LineItemReturnTuple.class,
            ReflectionUtils.specify(Tuple2.class),
            Integer.class
        );

        lineProjection.connectTo(0, coLJoin, 0);
        coJoin.connectTo(0, coLJoin, 1);

        MapOperator<Tuple2<LineItemReturnTuple, Tuple2<Integer, OrderReturnTuple>>, QueryResultTuple> joinProjection = new MapOperator<>(
                (coli -> new QueryResultTuple(
                    coli.field1.field1.O_ORDERKEY,
                    coli.field0.L_EXTENDEDPRICE,
                    coli.field1.field1.O_ORDERDATE,
                    coli.field1.field1.O_SHIPPRIORITY
                )),
                ReflectionUtils.specify(Tuple2.class),
                QueryResultTuple.class
        );
        coLJoin.connectTo(0, joinProjection, 0);

        ReduceByOperator<QueryResultTuple, GroupKey> aggregation = new ReduceByOperator<>(
                (returnTuple) -> new GroupKey(returnTuple.O_ORDERKEY, returnTuple.O_ORDERDATE, returnTuple.O_SHIPPRIORITY),
                ((t1, t2) -> {
                    t1.REVENUE += t2.REVENUE;
                    return t1;
                }),
                GroupKey.class,
                QueryResultTuple.class
        );

        // Print the results.
        joinProjection.connectTo(0, aggregation, 0);

        LocalCallbackSink<QueryResultTuple> sink = LocalCallbackSink.createStdoutSink(QueryResultTuple.class);
        aggregation.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}
